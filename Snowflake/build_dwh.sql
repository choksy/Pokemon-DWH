use role sysadmin;

create or replace warehouse deployment_wh
with WAREHOUSE_SIZE='X-SMALL'
MAX_CLUSTER_COUNT=1
MIN_CLUSTER_COUNT=1;

create or replace warehouse tasks_wh
with WAREHOUSE_SIZE='X-SMALL'
MAX_CLUSTER_COUNT=1
MIN_CLUSTER_COUNT=1;

use warehouse deployment_wh;

create or replace database Pokemons_Antropova;
use database Pokemons_Antropova;

create schema Pokemons_Antropova.staging;
create schema Pokemons_Antropova.storage;
create schema Pokemons_Antropova.data_marts;

------ setting up the staging schema -------------------------------------------------------------------------

use schema staging;

create or replace stage Antropova_S3_stage
  url='s3://de-school-snowflake/snowpipe/Antropova/'
  credentials=(aws_key_id='AKI_____' aws_secret_key='E59_______');

create or replace table stg_types (json_data variant);
create or replace table stg_moves (json_data variant);
create or replace table stg_species (json_data variant);
create or replace table stg_pokemons (json_data variant);

create or replace stream stg_types_stream on table stg_types;
create or replace stream stg_types_stream_2 on table stg_types;
create or replace stream stg_moves_stream on table stg_moves;
create or replace stream stg_moves_stream_2 on table stg_moves;
create or replace stream stg_species_stream on table stg_species;
create or replace stream stg_pokemons_stream on table stg_pokemons;
create or replace stream stg_pokemons_stream_2 on table stg_pokemons;
create or replace stream stg_pokemons_stream_3 on table stg_pokemons;
create or replace stream stg_pokemons_stream_4 on table stg_pokemons;

create or replace pipe types_load_pipe auto_ingest=true as
    copy into stg_types
    from (select $1
          from @Antropova_S3_stage/)
    file_format = (type = 'JSON'
                   strip_outer_array = true)
    pattern='.*types.*json';

create or replace pipe moves_load_pipe auto_ingest=true as
    copy into stg_moves
    from (select $1
          from @Antropova_S3_stage/)
    file_format = (type = 'JSON'
                   strip_outer_array = true)
    pattern='.*moves.*json';

create or replace pipe species_load_pipe auto_ingest=true as
    copy into stg_species
    from (select $1
          from @Antropova_S3_stage/)
    file_format = (type = 'JSON'
                   strip_outer_array = true)
    pattern='.*species.*json';

create or replace pipe pokemons_load_pipe auto_ingest=true as
    copy into stg_pokemons
    from (select $1
          from @Antropova_S3_stage/)
    file_format = (type = 'JSON'
                   strip_outer_array = true)
    pattern='.*pokemons.*json';

alter pipe types_load_pipe refresh;
alter pipe moves_load_pipe refresh;
alter pipe species_load_pipe refresh;
alter pipe pokemons_load_pipe refresh;

-- -- checking that everything worked as expected
-- select * from stg_types;
-- select * from stg_moves;
-- select * from stg_species;
-- select * from stg_pokemons;

------ setting up the storage schema -------------------------------------------------------------------------

use schema storage;

create or replace table types (name string primary key,
                               number_of_pokemons integer);

create or replace table moves (name string primary key,
                               number_of_pokemons integer);

create or replace table pokemons (name string primary key,
                                  generation string not null);

create or replace table pokemon_types (pokemon_name string references pokemons(name),
                                       type_name string references types(name),
                                       unique (pokemon_name, type_name));

create or replace table pokemon_moves (pokemon_name string references pokemons(name),
                                       move_name string references moves(name),
                                       unique (pokemon_name, move_name));

create or replace table pokemon_stats (pokemon_name string references pokemons(name),
                                       stat_name string not null,
                                       base_stat integer,
                                       unique (pokemon_name, stat_name));

select * from Pokemons_Antropova.staging.stg_types_stream;

create or replace task insert_types
  warehouse = 'tasks_wh'
  schedule = '5 minute'
when
  system$stream_has_data('Pokemons_Antropova.staging.stg_types_stream')
  AS
    insert into types (name, number_of_pokemons)
    select parse_json(t.$1):name::string as name,
           count(pokemons.value) as number_of_pokemons
    from Pokemons_Antropova.staging.stg_types_stream t,
    lateral flatten( input => $1:pokemons, outer => true) pokemons
    where t.metadata$action = 'INSERT'
    group by name;

select * from Pokemons_Antropova.staging.stg_types_stream;

create or replace task insert_moves
  warehouse = 'tasks_wh'
  schedule = '5 minute'
when
  system$stream_has_data('Pokemons_Antropova.staging.stg_moves_stream')
  AS
    insert into moves (name, number_of_pokemons)
    select parse_json(m.$1):name::string as name,
           count(*) as number_of_pokemons
    from Pokemons_Antropova.staging.stg_moves_stream m,
    lateral flatten( input => $1:pokemons) pokemons
    where m.metadata$action = 'INSERT'
    group by name;

create or replace task insert_pokemons
  warehouse = 'tasks_wh'
  schedule = '5 minute'
when
  system$stream_has_data('Pokemons_Antropova.staging.stg_pokemons_stream') and
  system$stream_has_data('Pokemons_Antropova.staging.stg_species_stream')
  AS
    insert into pokemons (name, generation)
    select parse_json(p.$1):name::string as name,
           parse_json(s.$1):generation::string as generation
    from Pokemons_Antropova.staging.stg_pokemons_stream p,
         Pokemons_Antropova.staging.stg_species_stream s
    where parse_json(p.$1):species_id = parse_json(s.$1):id and
          p.metadata$action = 'INSERT' and
          s.metadata$action = 'INSERT';

create or replace task insert_pokemon_stats
  warehouse = 'tasks_wh'
  schedule = '5 minute'
when
  system$stream_has_data('Pokemons_Antropova.staging.stg_pokemons_stream_2')
  AS
    insert into pokemon_stats (pokemon_name, stat_name, base_stat)
    select parse_json(p.$1):name::string as pokemon_name,
           stats.value:name::string as stat_name,
           stats.value:base::number as base_stat
    from Pokemons_Antropova.staging.stg_pokemons_stream_2 p,
    lateral flatten( input => $1:stats) stats
    where p.metadata$action = 'INSERT';

create or replace task insert_pokemon_types
  warehouse = 'tasks_wh'
  schedule = '5 minute'
when
  system$stream_has_data('Pokemons_Antropova.staging.stg_types_stream_2') and
  system$stream_has_data('Pokemons_Antropova.staging.stg_pokemons_stream_3')
  AS
    insert into pokemon_types (type_name, pokemon_name)
    select parse_json(t.$1):name::string as type_name,
           parse_json(p.$1):name::string as pokemon_name
    from Pokemons_Antropova.staging.stg_pokemons_stream_3 p,
         Pokemons_Antropova.staging.stg_types_stream_2 t,
    lateral flatten( input => (t.$1):pokemons) pokemons
    where parse_json(p.$1):id = pokemons.value and
          t.metadata$action = 'INSERT' and
          p.metadata$action = 'INSERT';

create or replace task insert_pokemon_moves
  warehouse = 'tasks_wh'
  schedule = '5 minute'
when
  system$stream_has_data('Pokemons_Antropova.staging.stg_moves_stream_2') and
  system$stream_has_data('Pokemons_Antropova.staging.stg_pokemons_stream_4')
  AS
    insert into pokemon_moves (move_name, pokemon_name)
    select parse_json(m.$1):name::string as move_name,
           parse_json(p.$1):name::string as pokemon_name
    from Pokemons_Antropova.staging.stg_moves_stream_2 m,
         Pokemons_Antropova.staging.stg_pokemons_stream_4 p,
    lateral flatten( input => (m.$1):pokemons) pokemons
    where parse_json(p.$1):id = pokemons.value and
          m.metadata$action = 'INSERT' and
          p.metadata$action = 'INSERT';

-- use role accountadmin;
-- GRANT EXECUTE TASK ON ACCOUNT TO ROLE sysadmin;
-- use role sysadmin;

execute task insert_types;
execute task insert_moves;
execute task insert_pokemons;
execute task insert_pokemon_stats;
execute task insert_pokemon_types;
execute task insert_pokemon_moves;

-- --checking that everything worked as expected
-- select * from types;
-- select * from moves;
-- select * from pokemons;
-- select * from pokemon_stats;
-- select * from pokemon_types;
-- select * from pokemon_moves;

------ setting up the experiments schema --------------------------------------------------------------------

use schema data_marts;

create or replace view pokemons_by_types as
select name as type,
       number_of_pokemons,
       number_of_pokemons - lag(number_of_pokemons) over(order by number_of_pokemons desc, name) as previous_type_difference,
       number_of_pokemons - lag(number_of_pokemons) over(order by number_of_pokemons, name desc) as next_type_difference
from Pokemons_Antropova.storage.types
order by number_of_pokemons desc, name;

create or replace view pokemons_by_moves as
select name as move,
       number_of_pokemons,
       number_of_pokemons - lag(number_of_pokemons) over(order by number_of_pokemons desc, name) as previous_move_difference,
       number_of_pokemons - lag(number_of_pokemons) over(order by number_of_pokemons, name desc) as next_move_difference
from Pokemons_Antropova.storage.moves
order by number_of_pokemons desc, name;

create or replace view pokemons_by_stats as
select p.name as pokemon,
       sum(base_stat) as stats_sum
from Pokemons_Antropova.storage.pokemon_stats ps,
     Pokemons_Antropova.storage.pokemons p
where p.name = ps.pokemon_name
group by p.name
order by stats_sum desc;

create or replace view pokemons_by_types_and_generations as
select * from (
    select p.generation,
           t.name as type,
           p.name as pokemon_name
      from Pokemons_Antropova.storage.types t
 left join Pokemons_Antropova.storage.pokemon_types pt
        on pt.type_name = t.name
 left join Pokemons_Antropova.storage.pokemons p
        on p.name = pt.pokemon_name
)
pivot(count(pokemon_name) for generation in('generation-i', 'generation-ii', 'generation-iii', 'generation-iv',
       'generation-v', 'generation-vi', 'generation-vii', 'generation-viii'))
  as p (type,generation_i,generation_ii,generation_iii,generation_iv,
       generation_v,generation_vi,generation_vii,generation_viii);
