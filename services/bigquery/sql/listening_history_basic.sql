select
    track.id as track_id,
    array(
        select
            name
        from unnest(track.artists)) as artists,
    track.name,
    track.album.name as album_name,
    track.duration_ms,
    played_at
from `{source}`
