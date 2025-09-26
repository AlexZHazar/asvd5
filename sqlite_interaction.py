import re

class sqlite_interaction:
    def __init__(self, sqlite_cursor):
        self.sqlite_cursor = sqlite_cursor
        self.output_log = ''
        self.warning_mark = '<span style="color: blue;">WARNING</span>'
        self.important_mark = '<span style="color: cyan;">IMPORTANT</span>'
        self.error_mark = '<span style="color: red;">ERROR</span>'

    def sqlite_checking_process(self):
        self.sqlite_cursor.execute("delete from tmp_generic_info")
        self.output_log += "-" * 40 + "<br>"
        for i in range(2):
            self.sqlite_cursor.execute(f"DELETE FROM tmp_generic_info{i+1}")
            self.sqlite_cursor.execute("DELETE FROM tmp_generic_info_tag")
            self.sqlite_cursor.execute(f"INSERT INTO tmp_generic_info_tag (first_tag, second_tag) VALUES ('inquiry_tag_#{i}', 'inquiry_tag_#{i+1}');")
            self.sqlite_cursor.execute(f"INSERT INTO tmp_generic_info{i+1} select * from tmp_generic_info_v;")
            self.sqlite_cursor.execute('COMMIT')
        self.sqlite_cursor.execute("""select c.source_file, c.carriage_id, t.repair_date_iso
    from tmp_generic_info1 t, tmp_carriage c
    where t.source_file not in(
    select t1.source_file from tmp_generic_info1 t1, tmp_generic_info2 t2
    where (t1.source_file = t2.source_file
    and t1.repair_date_iso = t2.repair_date_iso
    and cast(t1.factory_id as integer) = cast(t2.factory_id as integer))
    --repair_type_id = 5 =>
    union
    select t1.source_file from tmp_generic_info1 t1, tmp_generic_info2 t2
    where (t1.source_file = t2.source_file)
    and cast(t2.repair_type_id as integer) = 5
    ) and t.source_file = c.source_file""")
        rows = self.sqlite_cursor.fetchall()
        for row in rows:
            self.output_log += f"[{self.important_mark}] '{row[0]}' данные не финализированы - файл исключен:  {row[1]}  {row[2]}<br>"
            self.sqlite_cursor.execute(f"delete from tmp_xlsx_data where source_file = '{row[0]}'")
            self.sqlite_cursor.execute(f"delete from tmp_generic_info1 where source_file = '{row[0]}'")
        self.sqlite_cursor.execute("""INSERT INTO tmp_generic_info (
                             source_file,
                             repair_type,
                             repair_type_id,
                             repair_date_iso,
                             factory_name,
                             factory_id,
                             inquiry_date)
                             SELECT source_file,
                                    repair_type,
                                    repair_type_id,
                                    repair_date_iso,
                                    factory_name,
                                    factory_id,
                                    inquiry_date
                               FROM tmp_generic_info1
                               where cast(repair_type_id as integer) not in (5, 0);""")
        self.sqlite_cursor.execute("""INSERT INTO tmp_generic_info (
                             source_file,
                             repair_type,
                             repair_type_id,
                             repair_date_iso,
                             factory_name,
                             factory_id,
                             inquiry_date
                             )
                             SELECT t1.source_file,
                                    t2.repair_type,
                                    t2.repair_type_id,
                                    t2.repair_date_iso,
                                    t1.factory_name,
                                    t1.factory_id,
                                    t1.inquiry_date
                               FROM tmp_generic_info1 t1,
                                    tmp_generic_info2 t2
                              WHERE (t1.source_file = t2.source_file) AND
                                    CAST (t2.repair_type_id AS INTEGER) = 5 AND
                                    t1.repair_date_iso IS NULL;""")
        self.sqlite_cursor.execute('COMMIT')
        return self.output_log.strip('<br>'), 0

    def sqlite_loading_process(self):
        # self.output_log = ''

        self.sqlite_cursor.execute("DELETE FROM input_data WHERE performed = 0")

        self.sqlite_cursor.execute("""INSERT INTO input_data (source_file,
                           inquiry_date,
                           carriage_number,
                           carriage_build_year,
                           last_maintenance_type,
                           last_maintenance_type_name,
                           last_maintenance_date,
                           last_maintenance_factory,
                           last_maintenance_factory_name)
                           SELECT source_file,
                                  inquiry_date,
                                  carriage_number,
                                  carriage_build_year,
                                  last_maintenance_type,
                                  last_maintenance_type_name,
                                  last_maintenance_date,
                                  last_maintenance_factory,
                                  last_maintenance_factory_name
                             FROM input_data_import_py;""")

        self.sqlite_cursor.execute("""SELECT carriage_number, last_maintenance_date, source_file FROM input_data_import_py t1
        WHERE EXISTS (SELECT 1
                     FROM input_data t2
                    WHERE t2.carriage_number = t1.carriage_number AND
                          t2.last_maintenance_date = t1.last_maintenance_date AND
                          cast(t2.last_maintenance_factory as integer) = cast(t1.last_maintenance_factory as integer) AND
                          cast(t2.last_maintenance_type as integer) = cast(t1.last_maintenance_type as integer) AND
                          t2.performed = 1)""")
        rows = self.sqlite_cursor.fetchall()
        self.output_log += "-" * 40 + "<br>"
        for row in rows:
            self.output_log += f"[{self.warning_mark}] Файл '{row[2]}' содержит уже загруженные данные: {row[0]}  {row[1]}".strip('0:') + '<br>'

        self.sqlite_cursor.execute("""DELETE FROM inquiry_data_py
        WHERE EXISTS (SELECT 1
                     FROM input_data t2
                    WHERE t2.carriage_number = inquiry_data_py.carriage_number AND
                          t2.last_maintenance_date = inquiry_data_py.last_maintenance_date AND
                          cast(t2.last_maintenance_factory as integer) = cast(inquiry_data_py.last_maintenance_factory as integer) AND
                          cast(t2.last_maintenance_type as integer) = cast(inquiry_data_py.last_maintenance_type as integer) AND
                          t2.performed = 1)""")

        self.sqlite_cursor.execute("""DELETE FROM input_data
        WHERE performed = 0 AND
              EXISTS (SELECT 1
                     FROM input_data t2
                    WHERE t2.carriage_number = input_data.carriage_number AND
                          t2.last_maintenance_date = input_data.last_maintenance_date AND
                          cast(t2.last_maintenance_factory as integer) = cast(input_data.last_maintenance_factory as integer) AND
                          cast(t2.last_maintenance_type as integer) = cast(input_data.last_maintenance_type as integer) AND
                          t2.performed = 1)""")

        self.sqlite_cursor.execute("""WITH t AS (
    SELECT source_file,
           carriage,
           repair_date,
           cn,
           rn
      FROM (
               SELECT tt.*,
                      count(1) OVER (PARTITION BY carriage,
                      repair_type,
                      repair_date,
                      factory) AS cn,
                      row_number() OVER (PARTITION BY carriage,
                      repair_type,
                      repair_date,
                      factory ORDER BY source_file DESC) AS rn
                 FROM (
                          SELECT source_file,
                                 carriage_number AS carriage,
                                 CAST (last_maintenance_type AS INTEGER) AS repair_type,
                                 last_maintenance_date AS repair_date,
                                 CAST (last_maintenance_factory AS INTEGER) AS factory
                            FROM inquiry_data_py
                      )
                      tt
           )
     WHERE cn > 1
)
SELECT t1.source_file,
       t1.carriage,
       t2.source_file AS excluded_file,
       t1.repair_date
  FROM t AS t1,
       t AS t2
 WHERE t1.rn = 1 AND
       t2.rn > 1 AND
       t1.carriage = t2.carriage""")
        rows = self.sqlite_cursor.fetchall()
        if rows:
            self.output_log += "-" * 40 + "<br>"
            for row in rows:
                self.output_log += f"[{self.warning_mark}] Файл '{row[2]}' исключён, поскольку дублирует данные из '{row[0]}': {row[1]} {row[3]} ".strip('0:') + '<br>'
                self.sqlite_cursor.execute(f"delete from inquiry_data_py where source_file = '{row[2]}'")
                self.sqlite_cursor.execute(f"delete from input_data where source_file = '{row[2]}'")
            # self.output_log += "-" * 40 + "<br>"

        self.sqlite_cursor.execute("alter table inquiry_data_py add column order_number integer")

        self.sqlite_cursor.execute("""UPDATE inquiry_data_py
        SET order_number = t2.order_number
        FROM (SELECT source_file, order_number from input_data where source_file is not null and performed = 0) AS t2
        WHERE inquiry_data_py.source_file = t2.source_file;""")

        # Append_carriage -- replaced with trigger
        # Append_repairs:
        self.sqlite_cursor.execute("""INSERT INTO repairs (repair_date,
                        carriage_code,
                        repair_type,
                        factory,
                        axis11,
                        r_rim11,
                        l_rim11,
                        r_ridge11,
                        l_ridge11,
                        axis12,
                        r_rim12,
                        l_rim12,
                        r_ridge12,
                        l_ridge12,
                        beam1_year,
                        beam1,
                        r_frame1_year,
                        r_frame1,
                        l_frame1_year,
                        l_frame1,
                        axis21,
                        r_rim21,
                        l_rim21,
                        r_ridge21,
                        l_ridge21,
                        axis22,
                        r_rim22,
                        l_rim22,
                        r_ridge22,
                        l_ridge22,
                        beam2_year,
                        beam2,
                        r_frame2_year,
                        r_frame2,
                        l_frame2_year,
                        l_frame2,
                        axis_year11,
                        axis_year12,
                        axis_year21,
                        axis_year22,
                        axis_manufacturer11,
                        axis_manufacturer12,
                        axis_manufacturer21,
                        axis_manufacturer22,
                        beam1_manufacturer,
                        right_frame1_manufacturer,
                        left_frame1_manufacturer,
                        beam2_manufacturer,
                        right_frame2_manufacturer,
                        left_frame2_manufacturer,
                        order_number,
                        repair_date_iso)
                        SELECT idp.last_maintenance_date,
                               CAST (idp.carriage_number AS INTEGER) AS carriage_code,
                               idp.last_maintenance_type AS repair_type,
                               idp.last_maintenance_factory,
                               idp.axis_number11,
                               idp.right_wheel_rim11,
                               idp.left_wheel_rim11,
                               idp.right_wheel_ridge11,
                               idp.left_wheel_ridge11,
                               idp.axis_number12,
                               idp.right_wheel_rim12,
                               idp.left_wheel_rim12,
                               idp.right_wheel_ridge12,
                               idp.left_wheel_ridge12,
                               idp.beam1_year,
                               idp.beam1_number,
                               idp.right_frame1_year,
                               idp.right_frame1_number,
                               idp.left_frame1_year,
                               idp.left_frame1_number,
                               idp.axis_number21,
                               idp.right_wheel_rim21,
                               idp.left_wheel_rim21,
                               idp.right_wheel_ridge21,
                               idp.left_wheel_ridge21,
                               idp.axis_number22,
                               idp.right_wheel_rim22,
                               idp.left_wheel_rim22,
                               idp.right_wheel_ridge22,
                               idp.left_wheel_ridge22,
                               idp.beam2_year,
                               idp.beam2_number,
                               idp.right_frame2_year,
                               idp.right_frame2_number,
                               idp.left_frame2_year,
                               idp.left_frame2_number,
                               idp.axis_year11,
                               idp.axis_year12,
                               idp.axis_year21,
                               idp.axis_year22,
                               idp.axis_manufacturer11,
                               idp.axis_manufacturer12,
                               idp.axis_manufacturer21,
                               idp.axis_manufacturer22,
                               idp.beam1_manufacturer,
                               idp.right_frame1_manufacturer,
                               idp.left_frame1_manufacturer,
                               idp.beam2_manufacturer,
                               idp.right_frame2_manufacturer,
                               idp.left_frame2_manufacturer,
                               idp.order_number,
                               regexp_replace(idp.last_maintenance_date, '(\d{2})\.(\d{2})\.(\d{4})', '$3-$2-$1') 
                          FROM inquiry_data_py idp                      
                               LEFT JOIN
                               repairs r ON r.carriage_code = idp.carriage_number AND 
                                            CAST (r.repair_type AS INTEGER) = CAST (idp.last_maintenance_type AS INTEGER) AND 
                                            CAST (r.factory AS INTEGER) = CAST (idp.last_maintenance_factory AS INTEGER) AND 
                                            r.repair_date = idp.last_maintenance_date
                         WHERE r.carriage_code IS NULL; """)

        # Append_repairs_info_order_number --ignored
        # Append_factory -- new:
        self.sqlite_cursor.execute("""INSERT INTO factory (factory_code,
                    factory_name)
                    SELECT distinct substr('0000' || regexp_substr(t1.last_maintenance_factory, '\d+'), -4, 4) AS factory_code,
                           last_maintenance_factory_name
                      FROM inquiry_data_py t1
                     WHERE NOT EXISTS (SELECT 1
                                         FROM factory t2
                                        WHERE CAST (t2.factory_code AS INTEGER) = CAST (t1.last_maintenance_factory AS INTEGER) );""")

        self.sqlite_cursor.execute("""SELECT order_number, source_file , carriage_number, last_maintenance_date
        from input_data t1
        where performed = 0
        and not exists (select order_number from repairs t2 where t1.order_number = t2.order_number)""")
        rows = self.sqlite_cursor.fetchall()

        for row in rows:
            self.output_log += f"[{self.warning_mark}] Файл '{row[1]}' содержит уже загруженные данные: {row[2]}  {row[3]}".strip('0:') + '<br>'
            self.sqlite_cursor.execute(f"delete from input_data where order_number = {row[0]}")
            # print(f"delete from input_data where order_number = {row[0]}")

        self.output_log += "-" * 40 + "<br>"

        self.sqlite_cursor.execute("""SELECT source_file from input_data where performed = 0""")
        rows = self.sqlite_cursor.fetchall()

        for row in rows:
            self.output_log += f"[OK] Файл '{row[0]}' загружен<br>"
        if not rows:
            self.output_log += f"[{self.warning_mark}] Нет данных для загрузки<br>"
            # self.output_log += "-" * 40

        # Update_performed
        self.sqlite_cursor.execute("""UPDATE input_data AS t1 SET performed = 1
    WHERE performed = 0
    and exists (select 1 from repairs as t2 
    where cast(t1.carriage_number as integer) = t2.carriage_code
    and t1.last_maintenance_date = t2.repair_date
    and cast(t1.last_maintenance_factory as integer) = cast(t2.factory as integer)
    and cast(t1.last_maintenance_type as integer) = cast(t2.repair_type as integer));""")

        # tmp_repair_number_create -- ignored
        # Update_repair_number
        self.sqlite_cursor.execute("""WITH t AS (SELECT order_number,-- repair_number, repair_date, carriage_code,
                  row_number() OVER (PARTITION BY carriage_code ORDER BY regexp_replace(repair_date, '(\d+)\.(\d+)\.(\d+)', '$3$2$1') ) rn
             FROM repairs
            WHERE carriage_code IN (SELECT carriage_code
                                      FROM repairs
                                     WHERE repair_number IS NULL)
)
UPDATE repairs
   SET repair_number = (SELECT t.rn
                          FROM t
                         WHERE t.order_number = repairs.order_number)
 WHERE EXISTS (SELECT 1
                 FROM t
                WHERE t.order_number = repairs.order_number);""")

        self.sqlite_cursor.execute("COMMIT")
        # print(output_log)
        return self.output_log.strip('<br>'), 0

    def export_process(self):

        # self.sqlite_cursor.execute("SELECT name FROM PRAGMA_TABLE_INFO('tmp_eq_structure');")
        # columns = self.sqlite_cursor.fetchall()
        # col = [col[0] for col in columns]
        # print(col)

        self.sqlite_cursor.execute("select * from tmp_eq_structure;")
        rows = self.sqlite_cursor.fetchall()

        eq_script = 'create table tmp_eq as\nselect t.* from (\n'
        for row in rows:
            eq_script += f"""select
repair_date_iso, carriage_code, repair_number, '{row[4]}' as equ_type, '{row[5]}' as equ_num,
ifnull({row[7]},'') || '-' || ifnull({row[3]},'') || '-' || substr(ifnull({row[8]},''), -2) as equ,
{row[11]} as equ_res, {row[10]} as equ_err, {row[12]} as equ_action
from repairs
where {row[9]}{row[13]}
union all\n"""

        eq_script = re.sub(r'union all\n$', '', eq_script)+") as t;"
        self.sqlite_cursor.execute("DROP TABLE IF EXISTS tmp_eq;")
        self.sqlite_cursor.execute(eq_script)
        # print(eq_script)

        self.sqlite_cursor.execute("select * from tmp_eq_structure where stra = '1';")
        rows = self.sqlite_cursor.fetchall()

        eq_det_script = """create table tmp_eq_details as
select repair_date_iso, rt.repair_type, rn.carriage_code, rn.type, rn.owner, c.decommission, rn.factory, ifnull(repairs_factory, rn.factory) as repairs_factory, equ_type, equ, equ_res from (\n"""
        for row in rows:
            eq_det_script += f"""select rownum as carriage_rownum, r.carriage_code, type, owner, r.repair_date_iso, r.repair_type, r.factory, repairs_factory,
'{row[4]}{row[5]}' as equ_type, ifnull({row[7]},'') || '-' || ifnull(r.{row[3]},'') || '-' || substr(ifnull(r.{row[8]},''), -2) as equ, r.{row[11]} as equ_res
from export_repairs as er, repairs as r
where r.repair_number = er.repair_number and r.carriage_code = er.carriage_code and last_repair is not null
union all\n"""

        eq_det_script = re.sub(r'union all\n$', '', eq_det_script) + ") as rn\n"
        eq_det_script += """LEFT JOIN
   (SELECT value repair_code,
		   type repair_type
	  FROM dictionary
	 WHERE block = 'repair_type')
AS rt ON CAST (rn.repair_type AS INTEGER) = CAST (rt.repair_code AS INTEGER)
LEFT JOIN carriage c
on c.carriage_code = rn.carriage_code
order by carriage_rownum, replace(equ_type, 'к', 'а');"""

        self.sqlite_cursor.execute("DROP TABLE IF EXISTS tmp_eq_details;")
        self.sqlite_cursor.execute(eq_det_script)
        # print(eq_det_script)

        try:
            self.sqlite_cursor.execute("COMMIT")
        except:
            pass
