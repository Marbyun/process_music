DELETE_DATA = f"""
DELETE FROM datamart_music
"""

INSERT_DATA = f"""
with raw_data as (
select sd.code,
       case when sd.isrc is null then 'Not Found' else sd.isrc end isrc,
       sd.artist spotify_artist, ly.artist youtube_artist,
       sd.song_title spotify_song_title, ly.song_title youtube_song_title,
       ly.channel_id, ly.video_id,
       sd.album, sd.popularity, sd.release_date,
       CASE
           WHEN descriptions ~* '(?i)by|℗|From|Cipt\.' THEN
               split_part(
                       descriptions,
                       regexp_replace(
                               descriptions,
                               '(?i)(by|℗|From|Cipt\.).*', '\2'
                       ), 2
               )
           ELSE
               'Undefined'
           END AS descriptions
from spotify_datamart sd
left join  landing_youtube ly on upper(ly.song_title) = upper(sd.song_title)
        and upper(ly.artist) = upper(sd.artist)
)
, get_clean_data as (
select
code,
isrc,
case
    when youtube_artist is null
        then (
            case when spotify_artist = 'NULL' then 'Not Found' else spotify_artist end
        )
    else youtube_artist
end artist,
case
    when youtube_song_title is null
        then spotify_song_title
    else youtube_song_title
end song_title,
case when channel_id is null then 'Not Found' else channel_id end channel_id,
case when video_id is null then 'Not Found' else video_id end video_id,
case when album is null then 'Not Found' else album end album,
(case when popularity::text is null then 'Not Found' else popularity::text end) popularity,
descriptions
from raw_data
where isrc != 'Not Found' and channel_id is not null
)
, get_not_complete_data as (
select
    code,
    isrc,
    case
        when youtube_artist is null
            then (
            case when spotify_artist = 'NULL' then 'Not Found' else spotify_artist end
            )
        else youtube_artist
    end artist,
    case
        when youtube_song_title is null
            then spotify_song_title
        else youtube_song_title
    end song_title,
    case when channel_id is null then 'Not Found' else channel_id end channel_id,
    case when video_id is null then 'Not Found' else video_id end video_id,
    case when album is null then 'Not Found' else album end album,
    (case when popularity::text is null then 'Not Found' else popularity::text end) popularity,
    descriptions
from raw_data
where code not in (select code from get_clean_data)
)
, union_tab as (
select * from get_clean_data
union all
select * from get_not_complete_data
)
insert into public.datamart_music (code, isrc, artist, song_title, channel_id, video_id, album, popularity, descriptions)
select
code,
isrc,
artist,
song_title,
channel_id,
video_id,
album,
popularity,
descriptions
from union_tab
"""