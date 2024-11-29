DELETE_DATA = f"""
DELETE FROM public.spotify_datamart
"""

INSERT_DATA = f"""
with raw_data as (select dl.code,
                         ls.isrc,
                         ls.song_title spotify_title,
                         dl.song_title,
                         dl.original_artist,
                         ls.artist,
                         ls.album,
                         ls.release_date,
                         ls.popularity
                  from data_internal dl
                           left join landing_spotify ls on upper(dl.song_title) = upper(ls.song_title)
                                    and upper(dl.original_artist) = upper(ls.artist)
)
   , get_fill_isrc as (
    select
        code,
        isrc,
        spotify_title song_title,
        artist,
        album,
        release_date,
        popularity,
        row_number() over (partition by code ORDER BY release_date, popularity asc) rn
    from raw_data
    where isrc is not null)
, get_null_isrc as (select code,
                              isrc,
                              song_title,
                              original_artist artist,
                              album,
                              release_date,
                              popularity
                       from raw_data
                       where code not in (select code
                                          from get_fill_isrc
                                          where rn = 1))
   , union_tab as (
    select code, isrc, song_title, artist, album, release_date, popularity
    from get_fill_isrc where rn=1
    union all
    select code, isrc, song_title, artist, album, release_date, popularity
    from get_null_isrc
)
insert into public.spotify_datamart (code, isrc, song_title, artist, album, release_date, popularity)
select code,
       isrc,
       song_title,
       artist,
       album,
       release_date,
       popularity
from union_tab
"""