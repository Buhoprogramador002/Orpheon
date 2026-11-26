/* SPDX-License-Identifier: GPL-3.0-only
 *
 * Personal modification of Orpheon (https://github.com/kota-zo/Orpheon, formerly Myspoti,
 * https://github.com/kota-zo/myspoti), GPL-3.0: Liked Songs and playlists kept on the phone,
 * for use without a music account.
 */
package app.myspoti.api

import kotlinx.coroutines.flow.MutableSharedFlow
import kotlinx.coroutines.flow.SharedFlow
import kotlinx.coroutines.withTimeoutOrNull
import java.io.File
import java.net.URLDecoder
import java.net.URLEncoder
import java.time.Instant
import java.util.UUID

/**
 * The library of [NeutralApi] kept on this phone: saved items (Liked Songs) and the user's own
 * playlists, in one small text file. Songs are stored by uri with the details needed to list
 * them; missing details are read from the catalog through [trackLookup].
 *
 * The app names a catalog song `spotify:track:<provider>.<id>` (its catalog key) in most places
 * and `<provider>:track:<id>` in others: both are kept under the second form ([canonical]).
 */
class LocalLibrary {
    private class Row(val uid: String, val trackUri: Uri, val addedAt: Long)
    private class LocalList(val id: String, var name: String, var description: String, val createdAt: Long, val rows: MutableList<Row>)

    private val lock = Any()
    @Volatile private var file: File? = null
    @Volatile private var loaded = false

    /** Saved uri (canonical) → when it was saved (epoch ms), oldest first. */
    private val saved = LinkedHashMap<Uri, Long>()
    /** Details of the songs saved or put in a list, by canonical uri. */
    private val meta = HashMap<Uri, Track>()
    private val lists = LinkedHashMap<String, LocalList>()

    /** Reads a song's details from the catalog (set by the host). */
    @Volatile var trackLookup: (suspend (Uri) -> Track)? = null

    private val _changes = MutableSharedFlow<Unit>(extraBufferCapacity = 8)
    val changes: SharedFlow<Unit> get() = _changes

    /** Where the library is kept; read now (a change made before is kept too). */
    fun attach(f: File) {
        synchronized(lock) {
            file = f
            val before = Triple(LinkedHashMap(saved), HashMap(meta), LinkedHashMap(lists))
            saved.clear(); meta.clear(); lists.clear()
            runCatching { if (f.isFile) read(f.readLines()) }
            before.first.forEach { (k, v) -> saved.putIfAbsent(k, v) }
            before.second.forEach { (k, v) -> meta.putIfAbsent(k, v) }
            before.third.forEach { (k, v) -> lists.putIfAbsent(k, v) }
            loaded = true
            if (before.first.isNotEmpty() || before.third.isNotEmpty()) write()
        }
    }

    // ── Saved items (Liked Songs) ──

    fun isSaved(uri: Uri): Boolean = synchronized(lock) { canonical(uri) in saved }

    suspend fun save(uris: List<Uri>) {
        val keys = uris.map(::canonical)
        synchronized(lock) {
            val now = System.currentTimeMillis()
            keys.forEachIndexed { i, k -> if (k !in saved) saved[k] = now + i }
            write()
        }
        _changes.tryEmit(Unit)
        resolveMissing(keys.filter(::isTrack))
    }

    fun unsave(uris: List<Uri>) {
        synchronized(lock) {
            uris.map(::canonical).forEach { saved.remove(it) }
            write()
        }
        _changes.tryEmit(Unit)
    }

    suspend fun liked(offset: Int, limit: Int): Page<SavedTrack> {
        val all = synchronized(lock) { saved.entries.filter { isTrack(it.key) }.map { it.key to it.value }.asReversed().toList() }
        val slice = all.drop(offset).take(limit)
        resolveMissing(slice.map { it.first })
        val items = slice.map { (uri, at) -> SavedTrack(trackOf(uri).copy(addedAt = iso(at)), iso(at)) }
        val next = (offset + items.size).takeIf { it < all.size && items.isNotEmpty() }
        return Page(items, offset, all.size, next)
    }

    // ── Playlists ──

    fun isLocalList(uri: Uri): Boolean = uri.startsWith(LIST_PREFIX)

    fun playlists(): List<Playlist> = synchronized(lock) { lists.values.sortedByDescending { it.createdAt }.map(::playlistOf) }

    fun playlist(uri: Uri): Playlist? = synchronized(lock) { lists[idOf(uri)]?.let(::playlistOf) }

    fun items(sort: LibrarySort): List<LibraryItem> = synchronized(lock) {
        val all = lists.values.toList()
        val ordered = when (sort) {
            LibrarySort.ALPHABETICAL -> all.sortedBy { it.name.lowercase() }
            else -> all.sortedByDescending { it.createdAt }
        }
        ordered.map { l ->
            LibraryItem(uri = LIST_PREFIX + l.id, kind = MediaKind.PLAYLIST, name = l.name, subtitle = null,
                images = imagesOf(l), pinned = false, addedAt = iso(l.createdAt), count = l.rows.size)
        }
    }

    fun create(name: String): Uri {
        val id = UUID.randomUUID().toString().replace("-", "").take(16)
        synchronized(lock) {
            lists[id] = LocalList(id, name.trim().ifBlank { "Playlist" }, "", System.currentTimeMillis(), ArrayList())
            write()
        }
        _changes.tryEmit(Unit)
        return LIST_PREFIX + id
    }

    fun rename(uri: Uri, name: String) = edit(uri) { it.name = name.trim().ifBlank { it.name } }
    fun setDescription(uri: Uri, description: String) = edit(uri) { it.description = description }

    fun delete(uri: Uri) {
        synchronized(lock) {
            lists.remove(idOf(uri)) ?: throw NotAvailable(uri)
            write()
        }
        _changes.tryEmit(Unit)
    }

    suspend fun tracks(uri: Uri, offset: Int, limit: Int): Page<Track> {
        val rows = synchronized(lock) { lists[idOf(uri)]?.rows?.toList() } ?: throw NotAvailable(uri)
        val slice = rows.drop(offset).take(limit)
        resolveMissing(slice.map { it.trackUri }.distinct())
        val items = slice.map { r -> trackOf(r.trackUri).copy(uid = r.uid, addedAt = iso(r.addedAt)) }
        val next = (offset + items.size).takeIf { it < rows.size && items.isNotEmpty() }
        return Page(items, offset, rows.size, next)
    }

    suspend fun allTracks(uri: Uri): List<Track> {
        val size = synchronized(lock) { lists[idOf(uri)]?.rows?.size } ?: throw NotAvailable(uri)
        return tracks(uri, 0, maxOf(size, 1)).items
    }

    suspend fun addTracks(uri: Uri, trackUris: List<Uri>) {
        val keys = trackUris.map(::canonical)
        edit(uri) { l ->
            val now = System.currentTimeMillis()
            keys.forEachIndexed { i, k -> l.rows += Row(UUID.randomUUID().toString(), k, now + i) }
        }
        resolveMissing(keys.distinct())
    }

    fun removeTracks(uri: Uri, uids: List<String>) = edit(uri) { l -> l.rows.removeAll { it.uid in uids } }

    fun moveTracks(uri: Uri, uids: List<String>, beforeUid: String?) = edit(uri) { l ->
        val moving = l.rows.filter { it.uid in uids }
        l.rows.removeAll { it.uid in uids }
        val at = beforeUid?.let { b -> l.rows.indexOfFirst { it.uid == b } }?.takeIf { it >= 0 } ?: l.rows.size
        l.rows.addAll(at, moving)
    }

    // ── Helpers ──

    private fun edit(uri: Uri, change: (LocalList) -> Unit) {
        synchronized(lock) {
            val l = lists[idOf(uri)] ?: throw NotAvailable(uri)
            change(l)
            write()
        }
        _changes.tryEmit(Unit)
    }

    private fun idOf(uri: Uri) = uri.removePrefix(LIST_PREFIX)

    private fun playlistOf(l: LocalList) = Playlist(
        uri = LIST_PREFIX + l.id, name = l.name, description = l.description.ifBlank { null }, owner = null,
        images = imagesOf(l), trackCount = l.rows.size, editable = true,
    )

    /** The cover of the list's first song with one (its own cover is left to the app's mosaic otherwise). */
    private fun imagesOf(l: LocalList): List<Image> =
        l.rows.firstNotNullOfOrNull { r -> meta[r.trackUri]?.album?.images?.takeIf { it.isNotEmpty() } } ?: emptyList()

    private fun trackOf(uri: Uri): Track = synchronized(lock) { meta[uri] }
        ?: Track(uri = uri, name = uri.substringAfterLast(':'), durationMs = 0, artists = emptyList(), album = null)

    /** Reads from the catalog the details of the songs not known yet, and keeps them. */
    private suspend fun resolveMissing(uris: List<Uri>) {
        val lookup = trackLookup ?: return
        val missing = synchronized(lock) { uris.filter { isTrack(it) && it !in meta } }
        if (missing.isEmpty()) return
        var found = false
        for (u in missing) {
            val t = try {
                withTimeoutOrNull(LOOKUP_TIMEOUT_MS) { lookup(u) }
            } catch (e: kotlinx.coroutines.CancellationException) {
                throw e
            } catch (e: Exception) {
                null
            } ?: continue
            synchronized(lock) { meta[u] = t.copy(uri = u, uid = null, addedAt = null) }
            found = true
        }
        if (found) synchronized(lock) { write() }
    }

    // ── Storage: one record a line, tab-separated, each field URL-encoded ──

    private fun write() {
        val f = file ?: return
        if (!loaded) return
        val out = StringBuilder(HEADER).append('\n')
        val keep = HashSet<Uri>()
        saved.forEach { (k, at) -> line(out, "S", k, at.toString()); keep += k }
        lists.values.forEach { l ->
            line(out, "P", l.id, l.name, l.description, l.createdAt.toString())
            l.rows.forEach { r -> line(out, "R", l.id, r.uid, r.trackUri, r.addedAt.toString()); keep += r.trackUri }
        }
        meta.filterKeys { it in keep }.values.forEach { t ->
            line(out, "T", t.uri, t.name, t.durationMs.toString(), t.explicit.toString(),
                t.album?.uri.orEmpty(), t.album?.name.orEmpty(), t.album?.images?.firstOrNull()?.url.orEmpty(),
                t.artists.joinToString(ARTIST_SEP) { it.uri + PAIR_SEP + it.name })
        }
        runCatching {
            f.parentFile?.mkdirs()
            val tmp = File(f.parentFile, f.name + ".tmp")
            tmp.writeText(out.toString())
            if (!tmp.renameTo(f)) { f.writeText(out.toString()); tmp.delete() }
        }
    }

    private fun line(out: StringBuilder, kind: String, vararg fields: String) {
        out.append(kind)
        fields.forEach { out.append('\t').append(URLEncoder.encode(it, "UTF-8")) }
        out.append('\n')
    }

    private fun read(lines: List<String>) {
        if (lines.firstOrNull() != HEADER) return
        for (raw in lines.drop(1)) {
            val parts = raw.split('\t')
            if (parts.isEmpty()) continue
            val f = parts.drop(1).map { URLDecoder.decode(it, "UTF-8") }
            runCatching {
                when (parts[0]) {
                    "S" -> saved[f[0]] = f[1].toLong()
                    "P" -> lists[f[0]] = LocalList(f[0], f[1], f[2], f[3].toLong(), ArrayList())
                    "R" -> lists[f[0]]?.rows?.add(Row(f[1], f[2], f[3].toLong()))
                    "T" -> {
                        val artists = f[7].split(ARTIST_SEP).filter { it.isNotEmpty() }
                            .map { ArtistRef(it.substringBefore(PAIR_SEP), it.substringAfter(PAIR_SEP, "")) }
                        val album = f[4].takeIf { it.isNotEmpty() || f[5].isNotEmpty() }
                            ?.let { AlbumRef(f[4], f[5], listOfNotNull(f[6].takeIf { u -> u.isNotEmpty() }?.let { u -> Image(u) })) }
                        meta[f[0]] = Track(uri = f[0], name = f[1], durationMs = f[2].toLong(), artists = artists,
                            album = album, explicit = f[3].toBoolean())
                    }
                }
            }
        }
    }

    companion object {
        /** Uris of the phone's own playlists; the app reads a playlist's id after the last ':'. */
        const val LIST_PREFIX = "spotify:playlist:orpheonlocal"
        private const val HEADER = "ORPHEON-LOCAL-LIBRARY 1"
        private const val ARTIST_SEP = "\u0002"
        private const val PAIR_SEP = "\u0001"
        private const val LOOKUP_TIMEOUT_MS = 10_000L

        /** `spotify:track:deezer.123` → `deezer:track:123`; any other uri as it is. */
        fun canonical(uri: Uri): Uri {
            if (!uri.startsWith("spotify:track:")) return uri
            val key = uri.removePrefix("spotify:track:")
            val provider = key.substringBefore('.', "")
            return if (provider.isNotEmpty() && ':' !in key && provider.all { it.isLetterOrDigit() }) "$provider:track:${key.substringAfter('.')}" else uri
        }

        private fun isTrack(uri: Uri) = ":track:" in uri

        private fun iso(ms: Long): String = Instant.ofEpochMilli(ms).toString()
    }
}
