all_views = "SELECT name from sqlite_master WHERE type ='view';"

view_tmp_carriage = """CREATE VIEW tmp_carriage AS
    SELECT source_file,
           regexp_substr(A, '\d+$') carriage_id
      FROM tmp_xlsx_data
     WHERE regexp_like(A, (select initial from mapping where key = 'carriage_data'));"""

view_tmp_generic_info = """CREATE VIEW tmp_generic_info_v AS
WITH t AS (SELECT t1.A,
                  t1.C,
                  t1.source_file,
                  t1.id
             FROM tmp_xlsx_data t1
                  JOIN
                  (SELECT *
                     FROM (SELECT source_file,
                                  id + 1 AS id_start,
                                  lead(id) OVER (PARTITION BY source_file ORDER BY id) - 1 AS id_finish
                             FROM tmp_xlsx_data x
                            WHERE regexp_like(x.A, (select initial from mapping where key =
                                  (select first_tag from tmp_generic_info_tag))) OR 
                                  regexp_like(x.A, (select initial from mapping where key =
                                  (select second_tag from tmp_generic_info_tag))) )
                          xx
                    WHERE xx.id_finish IS NOT NULL)
                  t2 ON (t1.source_file = t2.source_file AND 
                         t1.id BETWEEN id_start AND id_finish) )
    SELECT ttt1.source_file,
           regexp_replace(ttt1.C, '[0-9)( ]+$', '') repair_type,
           substr('00' || regexp_substr(ttt1.C, '\d+'), -2, 2) repair_type_id,-- regexp_replace(ttt2.C, '(\d+)\D(\d+)\D(\d+)', '$3.$2.$1') repair_date,
           ttt2.C repair_date,
           regexp_replace(ttt3.C, '[0-9)( ]+$', '') factory_name,
           substr('0000' || regexp_substr(regexp_substr(ttt3.C, '\(\d+\)'), '\d+'), -4, 4) factory_id,
           inquiry_date
      FROM (SELECT tt1.C,
                   tt1.source_file
              FROM t AS tt1
             WHERE regexp_like(tt1.A, (select initial from mapping where key = 'repair_work_type')) )
           AS ttt1
           JOIN
           (SELECT tt3.C,
                   tt3.source_file
              FROM t AS tt3
             WHERE regexp_like(tt3.A, (select initial from mapping where key = 'repair_work_factory')) )
           AS ttt3 ON ttt1.source_file = ttt3.source_file
           LEFT JOIN
           (SELECT tt2.C,
                   tt2.source_file
              FROM t AS tt2
             WHERE regexp_like(tt2.A, (select initial from mapping where key = 'repair_work_date')) )
           AS ttt2 ON ttt1.source_file = ttt2.source_file
           JOIN
           (SELECT regexp_substr(A, '\S+') inquiry_date,
                   source_file
              FROM tmp_xlsx_data
             WHERE id = 1)
           AS ttt4 ON ttt1.source_file = ttt4.source_file; """

view_input_data_import_py="""CREATE VIEW input_data_import_py AS
    SELECT i.source_file,
           i.inquiry_date,
           c.carriage_id AS carriage_number,
           0 carriage_build_year,
           i.repair_type_id last_maintenance_type,
           i.repair_type last_maintenance_type_name,
           i.repair_date_iso last_maintenance_date, --???
           i.factory_id last_maintenance_factory,
           i.factory_name last_maintenance_factory_name
      FROM tmp_carriage c
           JOIN
           tmp_generic_info i ON c.source_file = i.source_file;"""

view_tmp_blocks = """CREATE VIEW tmp_blocks AS
WITH t AS (SELECT regexp_substr(A, '\d{2}') truck,
                  source_file,
                  id id_start,
                  ifnull(lead(id) OVER (PARTITION BY source_file ORDER BY id), 9999) - 1 id_finish
             FROM tmp_xlsx_data
            WHERE regexp_like(A, (select initial from mapping where key = 'inquiry_tag_truck')) )
    SELECT regexp_replace(
           regexp_replace(
           regexp_replace(t1.A,
           (select initial from mapping where key = 'axis_block'), 'axis'),
           (select initial from mapping where key = 'beam_block'), 'beam'), 
           (select initial from mapping where key = 'frame_block'), 'frame')
           block_type,
           t.truck,
           t1.source_file,
           t1.id id_start,
           ifnull(lead(t1.id) OVER (PARTITION BY t1.source_file), 9999) - 1 id_finish,
           id.order_number
      FROM (SELECT *
              FROM tmp_xlsx_data
             WHERE regexp_like(A, '^[124]\.') )
           AS t1
           JOIN
           t ON t1.source_file = t.source_file AND 
                t1.id BETWEEN t.id_start AND t.id_finish
           left join input_data id
           on t1.source_file = id.source_file;"""

view_tmp_axis = """CREATE VIEW tmp_axis AS
WITH t AS (SELECT t1.order_number,
                  t1.block_type,
                  t1.truck,
                  t1.id_start,
                  t1.id_finish,
                  t2.*
             FROM (SELECT *
                     FROM tmp_blocks
                    WHERE block_type = 'axis')
                  AS t1
                  JOIN
                  (SELECT *
                     FROM tmp_xlsx_data)
                  t2 ON t1.source_file = t2.source_file AND 
                        t2.id BETWEEN t1.id_start AND t1.id_finish),
tt as (SELECT order_number,
       block_type,
       id_start,
       id_finish,
       A,
       source_file,
       id,
       regexp_replace(
       regexp_replace(
       regexp_replace(
       regexp_replace(
       regexp_replace(
       regexp_replace(
       regexp_replace(
       t.A
       , (select initial from mapping where key = 'axis_year'), '##axis_year##') 
       , (select initial from mapping where key = 'axis_number'), '##axis_number##')
       , (select initial from mapping where key = 'axis_manufacturer'), '##axis_manufacturer##')
       , (select initial from mapping where key = 'right_wheel_rim'), '##right_wheel_rim##')
       , (select initial from mapping where key = 'left_wheel_rim'), '##left_wheel_rim##')
       , (select initial from mapping where key = 'right_wheel_ridge'), '##right_wheel_ridge##')
       , (select initial from mapping where key = 'left_wheel_ridge'), '##left_wheel_ridge##')
       data_type,
       substr(truck, -1, 1) part_x,
       C,
       D 
  FROM t)
select order_number,
    source_file,
    id,
    A,
    param, 
    value,
    case
    when data_type like '%wheel%' then regexp_substr(value, '^\d{2}')
    when data_type like '%manuf%' then substr('0000' || trim(regexp_substr(value, '\([0-9]+\)'), ')('), -4, 4)
    else value
    end fixed_value
from
(select order_number,
    source_file,
    id,
    A,
    data_type,
    replace(data_type, '#', '') || part param,
    trim(value) value
from
(SELECT order_number,
       block_type,
       id_start,
       id_finish,
       A,
       source_file,
       id,
       data_type,
       part_x || '1' part,
       C value
  FROM tt
UNION ALL
SELECT order_number,
       block_type,
       id_start,
       id_finish,
       A,
       source_file,
       id,
       data_type,
       part_x || '2' part,
       D value
  FROM tt)
  where data_type like '##%##');"""

view_tmp_beam = """CREATE VIEW tmp_beam AS
WITH t AS (SELECT t1.order_number,
                  t1.block_type,
                  t1.truck,
                  t1.id_start,
                  t1.id_finish,
                  t2.*
             FROM (SELECT *
                     FROM tmp_blocks
                    WHERE block_type = 'beam')
                  AS t1
                  JOIN
                  (SELECT *
                     FROM tmp_xlsx_data)
                  t2 ON t1.source_file = t2.source_file AND 
                        t2.id BETWEEN t1.id_start AND t1.id_finish)
select order_number,
    source_file,
    id,
    A,
    param, 
    value,
    case
    --when data_type like '%wheel%' then regexp_substr(value, '^\d{2}')
    when data_type like '%manuf%' then substr('0000' || trim(regexp_substr(value, '\([0-9]+\)'), ')('), -4, 4)
    else value
    end fixed_value
from
(select order_number,
    source_file,
    id,
    A,
    data_type,
    'beam' || part || replace(data_type, '#', '') param,
    trim(value) value
from
(SELECT order_number,
       block_type,
       id_start,
       id_finish,
       A,
       source_file,
       id,
       regexp_replace(
       regexp_replace(
       regexp_replace(
       t.A
       , (select initial from mapping where key = 'beam_year'), '##_year##') 
       , (select initial from mapping where key = 'beam_number'), '##_number##')
       , (select initial from mapping where key = 'beam_manufacturer'), '##_manufacturer##')
       data_type,
       substr(truck, -1, 1) part,
       C value
  FROM t)
  where data_type like '##%##');"""

view_tmp_frame = """CREATE VIEW tmp_frame AS
WITH t AS (SELECT t1.order_number,
                  t1.block_type,
                  t1.truck,
                  t1.id_start,
                  t1.id_finish,
                  t2.*
             FROM (SELECT *
                     FROM tmp_blocks
                    WHERE block_type = 'frame')
                  AS t1
                  JOIN
                  (SELECT *
                     FROM tmp_xlsx_data)
                  t2 ON t1.source_file = t2.source_file AND 
                        t2.id BETWEEN t1.id_start AND t1.id_finish),
tt as (SELECT order_number,
       block_type,
       id_start,
       id_finish,
       substr(truck, -1, 1) truck,
       A,
       source_file,
       id,
       regexp_replace(
       regexp_replace(
       regexp_replace(
       t.A
       , (select initial from mapping where key = 'frame_year'), '##year##') 
       , (select initial from mapping where key = 'frame_number'), '##number##')
       , (select initial from mapping where key = 'frame_manufacturer'), '##manufacturer##')
       data_type,
       C,
       D
  FROM t)
select * from
(select order_number,
    source_file,
    id,
    --truck,
    A,
    param, 
    value,
    case
    --when data_type like '%wheel%' then regexp_substr(value, '^\d{2}')
    when data_type like '%manuf%' then substr('0000' || trim(regexp_substr(value, '\([0-9]+\)'), ')('), -4, 4)
    else value
    end fixed_value,
    row_number() over(partition by source_file, param order by id) rn
from
(select order_number,
    source_file,
    id,
    truck,
    A,
    data_type,
    part || 'frame' || truck || '_' ||replace(data_type, '#', '') param,
    trim(value) value
from
(SELECT order_number,
       block_type,
       id_start,
       id_finish,
       truck,
       A,
       source_file,
       id,
       data_type,
       'left_' part,
       C value
  FROM tt
UNION ALL
SELECT order_number,
       block_type,
       id_start,
       id_finish,
       truck,
       A,
       source_file,
       id,
       data_type,
       'right_' part,
       D value
  FROM tt)
  where data_type like '##%##')
  order by source_file, id)
  where rn = 1;"""

view_tmp_equioment = """CREATE VIEW tmp_equipment AS
    SELECT order_number,
           source_file,
           param,
           ifnull(fixed_value, '0') AS value
      FROM tmp_axis
    UNION ALL
    SELECT order_number,
           source_file,
           param,
           ifnull(fixed_value, '0') AS value
      FROM tmp_beam
    UNION ALL
    SELECT order_number,
           source_file,
           param,
           ifnull(fixed_value, '0') AS value
      FROM tmp_frame;"""

view_tmp_carriage_info = """CREATE VIEW tmp_carriage_info_v AS
    SELECT c.key,
           d.value map_key,
           c.key || '_text' AS key_text,
           iif(c.value IS NULL, '', c.value) AS value,
           iif(d2.key IS NOT NULL, 0, 1) AS editable,
           iif(d3.key IS NOT NULL, 1, 0) AS boolean,
           iif(d4.key IS NOT NULL, 1, 0) AS list,
           iif(d5.key IS NOT NULL, 1, 0) AS date
      FROM tmp_carriage_info c
           LEFT JOIN
           dictionary d ON c.key = d.key AND 
                           d.block = 'carriage_info'
           LEFT JOIN
           dictionary d2 ON c.key = d2.key AND 
                            d2.block = 'carriage_info_param' AND 
                            d2.type = 'noneditable'
           LEFT JOIN
           dictionary d3 ON c.key = d3.key AND 
                            d3.block = 'carriage_info_param' AND 
                            d3.type = 'boolean'
           LEFT JOIN
           dictionary d4 ON c.key = d4.key AND 
                            d4.block = 'carriage_info_param' AND 
                            d4.type = 'list'
           LEFT JOIN
           dictionary d5 ON c.key = d5.key AND 
                            d5.block = 'carriage_info_param' AND 
                            d5.type = 'date'
     WHERE d.value IS NOT NULL
     ORDER BY row_number;"""

view_list_info = """CREATE VIEW list_info AS
    SELECT type as key,
           value
      FROM dictionary
     WHERE block = 'list'
    UNION ALL
    SELECT key, value from
    (SELECT distinct 'model' as key, model as value from carriage where model is not null order by model)
    UNION ALL
    SELECT key, value from
    (SELECT distinct 'caliber' as key, cast(caliber as text) as value from carriage where caliber is not null order by caliber)
    UNION ALL
    SELECT key, value from
    (SELECT distinct 'capacity' as key, cast(capacity as text) as value from carriage where capacity is not null order by capacity)
    --UNION ALL ...
    --UNION ALL SELECT 'factory', ''
    UNION ALL SELECT 'model', ''
    UNION ALL SELECT 'caliber', ''
    UNION ALL SELECT 'capacity', ''
    UNION ALL SELECT 'owner', ''
    UNION ALL SELECT 'type', '';"""

view_export_repairs = """CREATE VIEW export_repairs AS
    SELECT repairs.repair_date_iso AS repair_date_iso,
           IIf(r.last_repair > 0, '*', NULL) AS last_repair,
           repairs.repair_number,
           s.repairs_amount_between repair_seq,
           c3.rownum,
           repairs.carriage_code,
           c3.type,
           c3.owner,
           c3.decommission,
           c3.factory,
           c3.release_date,
           c3.model,
           c3.caliber,
           c3.capacity,
           NULL absorbing_device,
           NULL date_absorbing_device,
           NULL amount_absorbing_device,
           rt.repair_type,
           ifnull(f.factory_name, repairs.factory) AS repairs_factory,
           repairs.axis11,
           ifnull(repairs.r_rim11,'') || '-' || ifnull(repairs.l_rim11,'') AS rim11,
           IIf(repairs.r_rim11 < repairs.l_rim11, repairs.r_rim11, repairs.l_rim11) AS min_rim11,
           IIf(ifnull(repairs.r_ridge11,'') || '-' || ifnull(repairs.l_ridge11,'') = '-', '', ifnull(repairs.r_ridge11,'') || '-' || ifnull(repairs.l_ridge11,'')) AS ridge11,
           IIf(repairs.r_ridge11 < repairs.l_ridge11, repairs.r_ridge11, repairs.l_ridge11) AS min_ridge11,
           repairs.axis12,
           ifnull(repairs.r_rim12,'') || '-' || ifnull(repairs.l_rim12,'') AS rim12,
           IIf(repairs.r_rim12 < repairs.l_rim12, repairs.r_rim12, repairs.l_rim12) AS min_rim12,
           IIf(ifnull(repairs.r_ridge12,'') || '-' || ifnull(repairs.l_ridge12,'') = '-', '', ifnull(repairs.r_ridge12,'') || '-' || ifnull(repairs.l_ridge12,'')) AS ridge12,
           IIf(repairs.r_ridge12 < repairs.l_ridge12, repairs.r_ridge12, repairs.l_ridge12) AS min_ridge12,
           repairs.axis21,
           ifnull(repairs.r_rim21,'') || '-' || ifnull(repairs.l_rim21,'') AS rim21,
           IIf(repairs.r_rim21 < repairs.l_rim21, repairs.r_rim21, repairs.l_rim21) AS min_rim21,
           IIf(ifnull(repairs.r_ridge21,'') || '-' || ifnull(repairs.l_ridge21,'') = '-', '', ifnull(repairs.r_ridge21,'') || '-' || ifnull(repairs.l_ridge21,'')) AS ridge21,
           IIf(repairs.r_ridge21 < repairs.l_ridge21, repairs.r_ridge21, repairs.l_ridge21) AS min_ridge21,
           repairs.axis22,
           ifnull(repairs.r_rim22,'') || '-' || ifnull(repairs.l_rim22,'') AS rim22,
           IIf(repairs.r_rim22 < repairs.l_rim22, repairs.r_rim22, repairs.l_rim22) AS min_rim22,
           IIf(ifnull(repairs.r_ridge22,'') || '-' || ifnull(repairs.l_ridge22,'') = '-', '', ifnull(repairs.r_ridge22,'') || '-' || ifnull(repairs.l_ridge22,'')) AS ridge22,
           IIf(repairs.r_ridge22 < repairs.l_ridge22, repairs.r_ridge22, repairs.l_ridge22) AS min_ridge22,
           repairs.r_frame1,
           repairs.r_frame1_year,
           repairs.l_frame1,
           repairs.l_frame1_year,
           repairs.beam1,
           repairs.beam1_year,
           repairs.r_frame2,
           repairs.r_frame2_year,
           repairs.l_frame2,
           repairs.l_frame2_year,
           repairs.beam2,
           repairs.beam2_year,
           repairs.axis11_action,
           repairs.axis12_action,
           repairs.axis21_action,
           repairs.axis22_action,
           repairs.axis11_err,
           repairs.axis12_err,
           repairs.axis21_err,
           repairs.axis22_err,
           repairs.r_frame1_action,
           repairs.l_frame1_action,
           repairs.r_frame2_action,
           repairs.l_frame2_action,
           repairs.r_frame1_err,
           repairs.l_frame1_err,
           repairs.r_frame2_err,
           repairs.l_frame2_err,
           repairs.beam1_action,
           repairs.beam2_action,
           repairs.beam1_err,
           repairs.beam2_err
      FROM repairs
           JOIN
           (SELECT c1.*,
                   (SELECT count( * ) 
                      FROM carriage AS c2
                     WHERE c1.carriage_code >= c2.carriage_code AND 
                           c2.is_ignored = 0)
                   AS rownum
              FROM carriage AS c1
             WHERE c1.is_ignored = 0)
           AS c3 ON repairs.carriage_code = c3.carriage_code
           LEFT JOIN
           (SELECT count(1) AS last_repair,
                   carriage_code
              FROM repairs
             GROUP BY carriage_code)
           AS r ON (repairs.carriage_code = r.carriage_code) AND 
                   (repairs.repair_number = r.last_repair) 
           LEFT JOIN
           factory AS f ON CAST (repairs.factory AS INTEGER) = CAST (f.factory_code AS INTEGER) 
           LEFT JOIN
           (SELECT value repair_code,
                   type repair_type
              FROM dictionary
             WHERE block = 'repair_type')
           rt ON CAST (repairs.repair_type AS INTEGER) = CAST (rt.repair_code AS INTEGER) 
           LEFT JOIN
           tmp_export_repairs_seq AS s ON repairs.order_number = s.order_number
     ORDER BY c3.rownum,
              repairs.repair_number;"""

view_ttt_axis = """CREATE view ttt_axis AS
SELECT *
FROM (
    SELECT order_number,
           carriage_code,
           repair_type,
           --repair_type_name,
           repair_date,
           repair_date_iso,
           factory,
           --factory_name,
           axis_manufacturer11 AS axis_manufacturer,
           axis11 AS axis_number,
           axis_year11 AS axis_year,
           r_rim11 AS r_rim,
           l_rim11 AS l_rim,
           r_ridge11 AS r_ridge,
           l_ridge11 AS l_ridge,
           ifnull(axis_manufacturer11,'') || '-' || ifnull(axis11,'') || '-' || substr(ifnull(axis_year11,''), -2) AS axis
    FROM repairs

    UNION ALL

    SELECT order_number,
           carriage_code,
           repair_type,
           --repair_type_name,
           repair_date,
           repair_date_iso,
           factory,
           --factory_name,
           axis_manufacturer12,
           axis12,
           axis_year12,
           r_rim12,
           l_rim12,
           r_ridge12,
           l_ridge12,
           ifnull(axis_manufacturer12,'') || '-' || ifnull(axis12,'') || '-' || substr(ifnull(axis_year12,''), -2)
    FROM repairs

    UNION ALL

    SELECT order_number,
           carriage_code,
           repair_type,
           --repair_type_name,
           repair_date,
           repair_date_iso,
           factory,
           --factory_name,
           axis_manufacturer21,
           axis21,
           axis_year21,
           r_rim21,
           l_rim21,
           r_ridge21,
           l_ridge21,
           ifnull(axis_manufacturer21,'') || '-' || ifnull(axis21,'') || '-' || substr(ifnull(axis_year21,''), -2)
    FROM repairs

    UNION ALL

    SELECT order_number,
           carriage_code,
           repair_type,
           --repair_type_name,
           repair_date,
           repair_date_iso,
           factory,
           --factory_name,
           axis_manufacturer22,
           axis22,
           axis_year22,
           r_rim22,
           l_rim22,
           r_ridge22,
           l_ridge22,
           ifnull(axis_manufacturer22,'') || '-' || ifnull(axis22,'') || '-' || substr(ifnull(axis_year22,''), -2)
    FROM repairs
)
WHERE substr(axis, 1, 2) <> '--'
and order_number not in (select order_number from ttt_axis_exclude)
ORDER BY axis, repair_date_iso;"""

view_ttt_axis_full = """CREATE view ttt_axis_full AS
SELECT t4.*,
ROW_NUMBER() OVER (PARTITION BY axis ORDER BY repair_date_iso) - 1 as rownum
FROM (
    -- Использовалась
    SELECT order_number,
           carriage_code,
           repair_type,
           --repair_type_name,
           repair_date_iso,
           factory,
           --factory_name,
           axis,
           axis_manufacturer,
           axis_number,
           axis_year,
           r_rim,
           l_rim,
           r_ridge,
           l_ridge,
           'использовалась' AS status
    FROM ttt_axis

    UNION ALL

    -- Сняли
    SELECT NULL AS order_number,
           t3.carriage_code0 AS carriage_code,
           ta3.remove_type3 AS repair_type,
           --ta3.remove_type_name3 AS repair_type_name,
           t3.remove_date_iso2 AS repair_date_iso,
           ta3.remove_factory3 AS factory,
           --ta3.remove_factory_name3 AS factory_name,
           t3.axis0 AS axis,
           NULL AS axis_manufacturer,
           NULL AS axis_number,
           NULL AS axis_year,
           NULL AS r_rim,
           NULL AS l_rim,
           NULL AS r_ridge,
           NULL AS l_ridge,
           'сняли' AS status
    FROM (
        SELECT t2.m_date_iso,
               t2.axis0,
               t2.carriage_code0,
               MIN(t2.repair_date_iso1) AS remove_date_iso2
        FROM (
            SELECT t1.*,
                   ta1.repair_date_iso AS repair_date_iso1
            FROM (
                SELECT t0.m_date_iso,
                       t0.axis AS axis0,
                       ta0.carriage_code AS carriage_code0
                FROM (
                    SELECT MAX(repair_date_iso) AS m_date_iso,
                           axis
                    FROM ttt_axis
                    GROUP BY axis
                ) t0
                INNER JOIN ttt_axis ta0
                    ON ta0.axis = t0.axis
                   AND ta0.repair_date_iso = t0.m_date_iso
            ) t1
            INNER JOIN ttt_axis ta1
                ON t1.carriage_code0 = ta1.carriage_code
               AND ta1.repair_date_iso > t1.m_date_iso
        ) t2
        GROUP BY t2.m_date_iso, t2.axis0, t2.carriage_code0
    ) t3
    INNER JOIN (
        SELECT DISTINCT
               carriage_code AS carriage_code3,
               repair_date_iso AS remove_date_iso3,
               repair_type AS remove_type3,
               --repair_type_name AS remove_type_name3,
               factory AS remove_factory3
               --factory_name AS remove_factory_name3
        FROM ttt_axis
    ) ta3
    ON t3.carriage_code0 = ta3.carriage_code3
   AND t3.remove_date_iso2 = ta3.remove_date_iso3
) t4
ORDER BY axis, repair_date_iso;"""

view_ttt_axis_short = """CREATE VIEW ttt_axis_short AS
SELECT t6.*,
       last_r_rim - ROUND(le_duration * r_rim_delta) AS r_rim_expected,
       last_l_rim - ROUND(le_duration * l_rim_delta) AS l_rim_expected
FROM (
    SELECT t5.*,
           IIF(sl_duration = 0, 0, ABS(r_rim_diff) / sl_duration) AS r_rim_delta,
           IIF(sl_duration = 0, 0, ABS(l_rim_diff) / sl_duration) AS l_rim_delta
    FROM (
        SELECT t4.*,
               ROUND(julianday(last_date_iso) - julianday(start_date_iso)) AS sl_duration,
               start_r_rim - last_r_rim AS r_rim_diff,
               start_l_rim - last_l_rim AS l_rim_diff,
               ROUND(julianday(e_date_iso) - julianday(last_date_iso)) AS le_duration
        FROM (
            SELECT t3.*,
                   tt3.r_rim AS last_r_rim,
                   tt3.l_rim  AS last_l_rim
            FROM (
                SELECT t2.*,
                       tt2.r_rim AS start_r_rim,
                       tt2.l_rim  AS start_l_rim
                FROM (
                    SELECT t1.axis,
                           t1.start_date_iso,
                           t1.last_date_iso,
                           t1.end_date_iso,
                           t1.e_date_iso,
                           t1.status,
                           ROUND((julianday(e_date_iso) - julianday(start_date_iso)) / 365, 2) AS duration_years
                    FROM (
                        SELECT t0.*,
                               ta0.repair_date_iso AS end_date_iso,
                               IIF(ta0.repair_date_iso IS NULL,
                                   date('now'),
                                   ta0.repair_date_iso) AS e_date_iso,
                               ta0.status
                        FROM (
                            SELECT axis,
                                   MIN(repair_date_iso) AS start_date_iso,
                                   MAX(repair_date_iso) AS last_date_iso
                            FROM ttt_axis_full
                            WHERE status = 'использовалась'
                            GROUP BY axis
                        ) AS t0
                        LEFT JOIN (
                            SELECT *
                            FROM ttt_axis_full
                            WHERE status = 'сняли'
                        ) AS ta0 ON t0.axis = ta0.axis
                    ) AS t1
                ) AS t2
                LEFT JOIN ttt_axis_full AS tt2
                  ON t2.axis = tt2.axis
                 AND t2.start_date_iso = tt2.repair_date_iso
            ) AS t3
            LEFT JOIN ttt_axis_full AS tt3
              ON t3.axis = tt3.axis
             AND t3.last_date_iso = tt3.repair_date_iso
        ) AS t4
    ) AS t5
) AS t6;"""

view_ttt_axis_export = """CREATE view ttt_axis_export AS
SELECT *
  FROM (SELECT carriage_code,
               carriage_type,
               owner,
               decommission,
               r.repair_type,
               repair_date_iso,
               factory,
               factory_name,
               rownum,
               axis,
               axis_manufacturer,
               axis_number,
               axis_year,
               r_rim,
               l_rim,
               r_ridge,
               l_ridge,
               status,
               start_date_iso,
               end_date_iso,
               final_status,
               duration_years_full,
               r_rim_flag,
               l_rim_flag,
               sl_duration,
               r_rim_diff,
               l_rim_diff,
               le_duration,
               r_rim_delta,
               l_rim_delta,
               r_rim_expected,
               l_rim_expected
          FROM (SELECT carriage_code,
                       carriage_type,
                       owner,
                       decommission,
                       repair_type,
                       --repair_type_name,
                       repair_date_iso,
                       factory,
                       factory_name,
                       rownum,
                       axis,
                       axis_manufacturer,
                       axis_number,
                       axis_year,
                       r_rim,
                       l_rim,
                       r_ridge,
                       l_ridge,
                       status,
                       start_date_iso,
                       end_date_iso,
                       final_status,
                       duration_years AS duration_years_full,
                       iif(r_rim_flag0 = 1 AND 
                           (SELECT count(1) 
                              FROM ttt_axis_full x
                             WHERE x.axis = tt.axis AND 
                                   x.r_rim < 70 AND 
                                   x.rownum = tt.rownum - 1) > 0, 1, 0) AS r_rim_flag,
                       iif(l_rim_flag0 = 1 AND 
                           (SELECT count(1) 
                              FROM ttt_axis_full x
                             WHERE x.axis = tt.axis AND 
                                   x.l_rim < 70 AND 
                                   x.rownum = tt.rownum - 1) > 0, 1, 0) AS l_rim_flag,
                       sl_duration,
                       r_rim_diff,
                       l_rim_diff,
                       le_duration,
                       r_rim_delta,
                       l_rim_delta,
                       r_rim_expected,
                       l_rim_expected
                  FROM (SELECT carriage_code,
                               carriage_type,
                               owner,
                               decommission,
                               repair_type,
                               --repair_type_name,
                               repair_date_iso,
                               factory,
                               factory_name,
                               rownum,
                               t.axis,
                               axis_manufacturer,
                               axis_number,
                               axis_year,
                               r_rim,
                               l_rim,
                               r_ridge,
                               l_ridge,
                               IIf(round(julianday(repair_date_iso) - julianday(start_date_iso), 5) = 0, 'поставили', t.status) AS status,
                               s.start_date_iso,
                               s.end_date_iso,
                               s.status AS final_status,
                               s.duration_years,
                               iif(r_rim >= 70 AND 
                                   round(julianday(repair_date_iso) - julianday(start_date_iso), 5) > 0 AND 
                                   round(julianday(e_date_iso) - julianday(repair_date_iso) , 5)> 0 AND 
                                   t.status = 'использовалась', 1, 0) AS r_rim_flag0,
                               iif(l_rim >= 70 AND 
                                   round(julianday(repair_date_iso) - julianday(start_date_iso), 5) > 0 AND 
                                   round(julianday(e_date_iso) - julianday(repair_date_iso), 5) > 0 AND 
                                   t.status = 'использовалась', 1, 0) AS l_rim_flag0,
                               sl_duration,
                               r_rim_diff,
                               l_rim_diff,
                               le_duration,
                               r_rim_delta,
                               l_rim_delta,
                               r_rim_expected,
                               l_rim_expected
                          FROM (SELECT c.type as carriage_type,
                                       c.owner,
                                       c.decommission,
                                       ta.*,
                                       ifnull(f.factory_name, ta.factory) AS factory_name
                                  FROM ttt_axis_full AS ta
                                       LEFT JOIN
                                       carriage AS c ON ta.carriage_code = c.carriage_code
                                       LEFT JOIN factory as f on cast(f.factory_code as integer) = cast(ta.factory as integer)
                                 where c.is_ignored = 0
                                 )
                               AS t
                               LEFT JOIN
                               ttt_axis_short AS s ON t.axis = s.axis)
                       AS tt)
               AS tt2
               LEFT JOIN
               (SELECT value AS repair_type_code,
                       type AS repair_type
                  FROM dictionary
                 WHERE block = 'repair_type')
               AS r ON cast(tt2.repair_type as integer) = cast(r.repair_type_code as integer))
 /*where repair_date_iso > date('now', '-'||
       (select value from settings where key = 'eq_analysis_period_mon')||' months')*/
;"""

view_ttt_axis_export_full = """CREATE VIEW ttt_axis_export_full AS
    SELECT repair_date_iso,
           repair_type,
           carriage_code,
           carriage_type,
           owner,
           decommission,
           factory,
           factory_name,
           rownum,
           axis,
           axis_manufacturer,
           axis_number,
           axis_year,
           r_rim,
           l_rim,
           r_ridge,
           l_ridge,
           status,
           start_date_iso,
           end_date_iso,
           final_status,
           duration_years_full,
           r_rim_flag,
           l_rim_flag,
           sl_duration,
           r_rim_diff,
           l_rim_diff,
           le_duration,
           r_rim_delta,
           l_rim_delta,
           r_rim_expected,
           l_rim_expected
      FROM ttt_axis_export
     ORDER BY axis,
              repair_date_iso;"""

view_ttt_axis_export_final = """create view ttt_axis_export_final as
SELECT *
FROM ttt_axis_export
WHERE status='поставили'
order by axis, repair_date_iso;"""

view_ = """CREATE VIEW tmp_eq_structure AS
with t as (
select
t2.*,
row_number() over(partition by sub_type, type order by nnx) as rn
from
(SELECT t1.*,
       CASE type || ifnull(sub_type, '') WHEN 'axis' THEN 'кп' WHEN 'frame' THEN 'бр' WHEN 'beam' THEN 'нб' END AS ru_type,
       CASE type || ifnull(sub_type, '') WHEN 'axis' THEN 1 WHEN 'frame' THEN 2 WHEN 'beam' THEN 3 END AS ord,
       type || regexp_substr(name, '\d+') || CASE substr(name, 1, 2) WHEN 'r_' THEN '2' WHEN 'l_' THEN '1' WHEN 'ri' THEN '2' WHEN 'le' THEN '1' ELSE '' END AS nnx,
       regexp_substr(name, '\d+') || CASE substr(name, 1, 2) WHEN 'r_' THEN '2' WHEN 'l_' THEN '1' WHEN 'ri' THEN '2' WHEN 'le' THEN '1' ELSE '' END AS nn
  FROM (SELECT name,
               regexp_substr(name, 'axis|frame|beam') type,
               regexp_substr(name, 'year|manufacturer|action|err') sub_type
          FROM PRAGMA_TABLE_INFO('repairs') 
         WHERE regexp_like(name, 'axis|frame|beam') )
       AS t1) as t2),
z as
(select 'axis' as type, 'r_rimX || ''-'' || l_rimX' as str0
union all
select '^(f|b)' as type, null as str0
),
x as
(select 'iif(' as str1, null as str2, ' = 1, 1, 2)' as str3, ' > 0' as strx
union all
select '' as str1, '1' as str2, '' as str3, ' >= 3' as strx)
--
select
tt.*,
iif(z.str0 is null, year, replace(z.str0, 'X', nn)) as str0,
x.str1 || ifnull(x.str2, tt.action) || x.str3 as stra,
x.strx
from
(select
    nnx,
    nn,
    type,
    name,
    ru_type,
    rn,
    ord,
    manufacturer,
    year,
    action,
    err
from
(select nnx as nnx, nn, type, name, ru_type, rn, ord from t 
where sub_type is null) as tt1,
(select nnx as nnx2, name as manufacturer from t 
where sub_type = 'manufacturer') as tt2,
(select nnx as nnx3, name as year from t 
where sub_type = 'year') as tt3,
(select nnx as nnx4, name as action from t 
where sub_type = 'action') as tt4,
(select nnx as nnx5, name as err from t 
where sub_type = 'err') as tt5
where tt1.nnx = tt2.nnx2
and tt1.nnx = tt3.nnx3
and tt1.nnx = tt4.nnx4
and tt1.nnx = tt5.nnx5) as tt, x, z
where regexp_like(tt.type, z.type)
order by ord, strx, rn;"""

view_tmp_repairs_amount = """CREATE view tmp_repairs_amount AS
SELECT tttt.carriage_code,
       tttt.repair_max1,
       tttt.repairs_amount
  FROM (SELECT ttt.carriage_code,
               ttt.repair_max1,
               IIF(ttt.repairs_amount >= 0 AND 
                   ttt.repairs_amount < 3, NULL, ttt.repairs_amount) AS repairs_amount
          FROM (SELECT tt.carriage_code,
                       tt.repair_max1,
                       IIF(repair_max12 = 0 AND 
                           repair_max23 <> 0, repair_max3 - repair_max1, IIF(repair_max12 <> 0, repair_max2 - repair_max1 - 1, NULL) ) AS repairs_amount
                  FROM (SELECT t1.carriage_code,
                               t1.repair_max1,
                               t2.repair_max2,
                               t3.repair_max3,
                               t2.repair_max2 - t1.repair_max1 AS repair_max12,
                               t3.repair_max3 - t2.repair_max2 AS repair_max23
                          FROM (SELECT carriage_code,
                                       MAX(repair_number) AS repair_max1
                                  FROM repairs
                                 WHERE cast(repair_type as integer) IN (1, 2) AND 
                                       repair_date_iso < date('now', '-2 months') 
                                 GROUP BY carriage_code)
                               AS t1
                               INNER JOIN
                               (SELECT carriage_code,
                                       MAX(repair_number) AS repair_max2
                                  FROM repairs
                                 WHERE cast(repair_type as integer) IN (1, 2) 
                                 GROUP BY carriage_code)
                               AS t2 ON t1.carriage_code = t2.carriage_code
                               INNER JOIN
                               (SELECT carriage_code,
                                       MAX(repair_number) AS repair_max3
                                  FROM repairs
                                 GROUP BY carriage_code)
                               AS t3 ON t1.carriage_code = t3.carriage_code)
                       AS tt)
               AS ttt)
       AS tttt
 WHERE tttt.repairs_amount IS NOT NULL;"""

view_tmp_export_repairs_seq = """create view tmp_export_repairs_seq as
SELECT 
    r.order_number,
    IIF(
        r.repair_number >= ttt.repair_max1 
        AND r.repair_number <= ttt.repair_max1 + ttt.repairs_amount,
        ttt.repairs_amount,
        NULL
    ) AS repairs_amount_between
FROM repairs AS r
JOIN tmp_repairs_amount AS ttt
    ON r.carriage_code = ttt.carriage_code;"""

view_tmp_eq_last = """CREATE view tmp_eq_last AS
SELECT t1.*,
       ROW_NUMBER() OVER (PARTITION BY t1.carriage_code,
       repair_number,
       equ_type,
       equ_action ORDER BY IIf(IIf(equ_err IS NULL, 0, equ_err) = 0, 0, 1),
       substr(equ, 1, 4),
       equ_num) AS rownum
  FROM tmp_eq AS t1
       INNER JOIN
       (SELECT carriage_code,
               MIN(repair_number) - 1 AS mirn
          FROM tmp_eq
         WHERE repair_date_iso > datetime('now', '-' || (SELECT value
                                                           FROM settings
                                                          WHERE key = 'eq_analysis_period_mon')
||                                        ' months') 
         GROUP BY carriage_code)
       AS t2 ON t1.carriage_code = t2.carriage_code AND 
                t1.repair_number >= t2.mirn;"""

view_export_equipment = """create view export_equipment as
SELECT rn.repair_date_iso,
       rn.carriage_code,
       rn.type AS carriage_type,
       rn.owner,
       decommission,
       iif(max(rn.repair_number) over(partition by rn.carriage_code) = rn.repair_number, '*', null) as last_repair,
       rn.repair_number,
       rt.repair_type,
       rn.factory,
       rn.factory_name,
       rn.equ_type,
       terp.equ AS old_equ,
       terp.equ_res AS old_equ_res,
       rn.equ AS new_equ,
       rn.equ_res AS new_equ_res,
       terp.equ_err AS old_equ_err,
       rn.equ_err AS new_equ_err
  FROM (SELECT t.*,
               ter.equ_type,
               ter.equ,
               ter.equ_res,
               ter.equ_err,
               ter.equ_action,
               ter.rownum
          FROM (SELECT r.repair_date_iso,
                       r.carriage_code,
                       r.repair_number,
                       r.repair_type,
                       c.owner,
                       c.type,
                       r.factory,
                       ifnull(f.factory_name, r.factory) AS factory_name,
                       c.decommission
                  FROM carriage AS c
                       INNER JOIN
                       (repairs AS r
                       LEFT JOIN
                       factory AS f ON r.factory = f.factory_code)
                       ON c.carriage_code = r.carriage_code
                       where is_ignored = 0)
               AS t
               INNER JOIN
               tmp_eq_last AS ter ON t.carriage_code = ter.carriage_code AND 
                                     t.repair_number = ter.repair_number
         WHERE ter.equ_action = 1)
       AS rn
       LEFT JOIN
           (SELECT value repair_code,
                   type repair_type
              FROM dictionary
             WHERE block = 'repair_type')
       AS rt ON CAST (rn.repair_type AS INTEGER) = CAST (rt.repair_code AS INTEGER) 
       LEFT JOIN
       (SELECT *
          FROM tmp_eq_last
         WHERE equ_action = 2)
       AS terp ON rn.carriage_code = terp.carriage_code AND 
                  rn.repair_number = terp.repair_number + 1 AND 
                  rn.equ_type = terp.equ_type AND 
                  rn.rownum = terp.rownum
 ORDER BY rn.repair_date_iso DESC,
          rn.carriage_code DESC,
          rn.repair_number DESC,
          rn.equ_type,
          rn.equ;"""

view_export_equipment_details = "create view export_equipment_details as select * from tmp_eq_details;"

view_export_axis_analysis_final = """CREATE VIEW export_axis_analysis_final AS
    SELECT t1.repair_date_iso,
           t1.repair_type,
           t1.carriage_code,
           t1.carriage_type,
           t1.owner,
           t1.decommission,
           t1.factory,
           ifnull(t1.factory_name, t1.factory) AS factory_name,
           t1.rownum,
           t1.axis,
           t1.axis_manufacturer,
           t1.axis_number,
           t1.axis_year,
           t1.r_rim,
           t1.l_rim,
           t1.r_ridge,
           t1.l_ridge,
           t1.status,
           t1.start_date_iso,
           t1.end_date_iso,
           t1.final_status,
           t2.factory as factory2,
           ifnull(t2.factory_name, t2.factory) as factory_name2,
           t2.l_rim_expected,
           t2.r_rim_expected,
           t1.duration_years_full,
           t1.r_rim_flag,
           t1.l_rim_flag,
           t1.sl_duration,
           t1.r_rim_diff,
           t1.l_rim_diff,
           t1.le_duration,
           t1.r_rim_delta,
           t1.l_rim_delta,
           t1.r_rim_expected as r_rim_expected2,
           t1.l_rim_expected as l_rim_expected2
      FROM ttt_axis_export_final AS t1
           LEFT JOIN
           ttt_axis_export_full AS t2 ON t1.axis = t2.axis AND 
                                         t2.status = 'сняли'
order by regexp_replace(t1.axis, '^-', '');"""

view_export_axis_analysis_full = """CREATE VIEW export_axis_analysis_full AS
select * from ttt_axis_export_full
order by regexp_replace(axis, '^-', '');"""

view_ttt_axis_exclude = """create view ttt_axis_exclude as
select order_number
FROM repairs r where exists
(SELECT 1
FROM (
    SELECT carriage_code, repair_date_iso
    FROM (
        SELECT t1.carriage_code,
               t1.repair_date_iso,
               t2.repair_date_iso AS repair_date_iso2,
               CAST(STRFTIME('%Y', t2.repair_date_iso) AS INTEGER) - CAST(STRFTIME('%Y', t1.repair_date_iso) AS INTEGER) AS ddiff
        FROM (
            SELECT carriage_code, repair_date_iso
            FROM repairs
            WHERE repair_number = 1
        ) AS t1
        INNER JOIN (
            SELECT carriage_code, repair_date_iso
            FROM repairs
            WHERE repair_number = 2
        ) AS t2
        ON t1.carriage_code = t2.carriage_code
    ) AS tt
    WHERE ddiff > 4 OR ddiff IS NULL
) AS t2
WHERE r.carriage_code = t2.carriage_code
  AND DATE(r.repair_date_iso) = DATE(t2.repair_date_iso))"""

# ================================================

table_tmp_generic_info = """CREATE TABLE IF NOT EXISTS tmp_generic_info 
                              (source_file    TEXT,
                               repair_type    TEXT,
                               repair_type_id TEXT,
                               repair_date_iso    TEXT,
                               factory_name   TEXT,
                               factory_id     TEXT,
                               inquiry_date   TEXT);"""

table_tmp_generic_info1 = """CREATE TABLE IF NOT EXISTS tmp_generic_info1
                               (source_file    TEXT,
                                repair_type    TEXT,
                                repair_type_id TEXT,
                                repair_date_iso    TEXT,
                                factory_name   TEXT,
                                factory_id     TEXT,
                                inquiry_date   TEXT);"""

table_tmp_generic_info_tag = """CREATE TABLE IF NOT EXISTS tmp_generic_info_tag 
                                  (first_tag  TEXT,
                                   second_tag TEXT);"""

table_tmp_generic_info2 = """CREATE TABLE IF NOT EXISTS tmp_generic_info2
                               (source_file    TEXT,
                                repair_type    TEXT,
                                repair_type_id TEXT,
                                repair_date_iso    TEXT,
                                factory_name   TEXT,
                                factory_id     TEXT,
                                inquiry_date   TEXT);"""
