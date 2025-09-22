import re


class sqlite_analysis:
    def __init__(self, sqlite_conn, sqlite_cursor, flag_full=1):
        self.sqlite_conn = sqlite_conn
        self.sqlite_cursor = sqlite_cursor
        self.flag_full = flag_full
        # print(flag_full)
        # print(int(flag_full))
        self.sqlite_cursor.execute(f"update settings set value = {int(flag_full)} where key = 'full_analysis'")
        self.sqlite_cursor.execute('COMMIT')
        self.equipment = {
            'axis':('axis11','axis12','axis21','axis22'),
            'frame':('r_frame1','l_frame1','r_frame2','l_frame2'),
            'beam':('beam1','beam2')
        }
        # self.output_log = ''

    def sqlite_analysis_process(self):

        """
        The method performs the analysis of the equipment in the database and
        marks the repairs that require attention.

        It creates a temporary table `tmp_{key}_action` and fills it with data
        from the `repairs` table. The data is filtered by the equipment and the
        status of the repair (finished or not). The method then updates the
        `repairs` table with the data from the temporary table.

        The method iterates over the equipment and creates a temporary table
        for each one. It then fills the table with data from the `repairs`
        table and updates the `repairs` table with the data from the temporary
        table.

        The method finally commits the changes to the database.

        :return: None
        """
        for key, value in self.equipment.items():

            script0_drop = f"drop table if exists tmp_{key}_action;"

            script1_text01 = f"""create table tmp_{key}_action as
select order_number,\n"""

            script1_text02 = ''

            script1_text03 = """  from (select order_number,
               carriage_code,
               repair_number,\n"""

            script1_text04 = ''
            script1_text04_1 = ''
            script1_text04_2 = ''
            script1_text04_3 = ''
            script1_text05 = ''
            script1_text05_1 = ''
            script1_text05_2 = ''

            script1_text06 = """     from (select order_number,
                       carriage_code,
                       repair_number,
                       max_repair_number ,\n"""

            script1_text07 = ''
            script1_text07_1 = ''
            script1_text07_2 = ''
            script1_text07_3 = ''
            script1_text08 = ''
            script1_text09 = ''

            script1_text10 = """                  from (SELECT r.order_number,
                               r.carriage_code,
                               r.repair_number,
                               max_repair_number,\n"""

            script1_text11 = ''
            script1_text11_1 = ''
            script1_text11_2 = ''
            script1_text11_3 = ''
            script1_text12 = ''
            script1_text13 = ''

            script1_text14 = f"""                          FROM (((select *
                                    from repairs
                                   where carriage_code in
                                         (SELECT distinct carriage_code
                                            FROM repairs
                                           where ({value[0]}_action is null)
                                              or not
                                                  ((SELECT value FROM settings where key = 'full_analysis')='0'))) AS r LEFT JOIN
                                repairs AS rn
                                ON(r.repair_number = rn.repair_number - 1) AND
                                (r.carriage_code = rn.carriage_code)) LEFT JOIN
                                repairs AS rp
                                ON(r.carriage_code = rp.carriage_code) AND
                                (r.repair_number = rp.repair_number + 1))
                          left join (select rrm.carriage_code,
                                           max(rrm.repair_number) as max_repair_number
                                      from (select *
                                    from repairs
                                   where carriage_code in
                                         (SELECT distinct carriage_code
                                            FROM repairs
                                           where ({value[0]}_action is null)
                                              or not
                                                  ((SELECT value FROM settings where key = 'full_analysis')='0'))) as rrm
                                     group by carriage_code) as rm
                            on (r.carriage_code = rm.carriage_code)
                         ORDER BY r.carriage_code, r.repair_number) as r1) as r2) as r3;"""

            script2_text01 = 'UPDATE repairs SET\n'

            script2_text02 = ''

            script2_text03 = f"""              FROM tmp_{key}_action AS t
             WHERE repairs.order_number = t.order_number;"""

            script3_text01 = 'UPDATE repairs SET\n'
            
            script3_text02 = ''
            script3_text03 = ''
            script3_text03_fix = ''

            script4_drop = f"drop table if exists tmp_{key}_analysis;"

            for i in value:
                # print(i)
                script1_text02 += f"       {i}_p_action + {i}_n_action as {i}_action,\n"

                script1_text04_1 += f"               r_{i},\n"
                script1_text04_2 += f"               p1_{i},\n"
                script1_text04_3 += f"               n1_{i},\n"

                script1_text05_1_value = "||'|'||p1_".join(value)
                script1_text05_1 += f"""iif(regexp_like(r_{i},'\d'),iif(regexp_like(r_{i},'^('||p1_{script1_text05_1_value}||')$'),0,1),-1) as {i}_p_action,\n"""
                script1_text05_2_value = "||'|'||n1_".join(value)
                script1_text05_2 += f"""iif(regexp_like(r_{i},'\d'),iif(regexp_like(r_{i},'^('||n1_{script1_text05_2_value}||')$'),0,2),-1) as {i}_n_action,\n"""

                script1_text07_1 += f"                       r_{i},\n"
                script1_text07_2 += f"                       p_{i},\n"
                script1_text07_3 += f"                       n_{i},\n"

                script1_text08 += f"                       iif(rp_{key} is null and repair_number = 1, r_{i}, p_{i}) as p1_{i},\n"
                script1_text09 += f"                       iif(rn_{key} is null and repair_number = max_repair_number, r_{i}, n_{i}) as n1_{i},\n"

                script1_text11_1 += f"                               iif(r.{i}  is null, 'r', r.{i} ) as r_{i},\n"
                script1_text11_2 += f"                               iif(rp.{i} is null, 'p', rp.{i}) as p_{i},\n"
                script1_text11_3 += f"                               iif(rn.{i} is null, 'n', rn.{i}) as n_{i},\n"

                script2_text02 += f"       {i}_action = t.{i}_action,\n"

                script3_text02 += f"       {i}_action = 4,\n"
                script3_text03_fix += '3'



            script1_text02 = re.sub(r',\n$','',script1_text02)+"\n"
            script1_text04 = script1_text04_1+script1_text04_2+script1_text04_3
            script1_text05 = re.sub(r',\n$','',script1_text05_1+script1_text05_2)+"\n"
            script1_text07 = script1_text07_1 + script1_text07_2 + script1_text07_3
            script1_text09 = re.sub(r',\n$', '', script1_text09) + "\n"
            script1_text11 = script1_text11_1 + script1_text11_2 + script1_text11_3
            script1_text12_value = " ||rp.".join(value)
            script1_text12 = f"                               rp.{script1_text12_value} as rp_{key},\n"
            script1_text13_value = " ||rn.".join(value)
            script1_text13 = f"                               rn.{script1_text13_value} as rn_{key}\n"

            script2_text02 = re.sub(r',\n$', '', script2_text02) + "\n"

            script3_text02 = re.sub(r',\n$', '', script3_text02) + "\n"
            script3_text03_value = "_action || ".join(value)
            script3_text03 = f" WHERE {script3_text03_value}_action = '{script3_text03_fix}';"

            script1_list = []
            script2_list = []
            script3_list = []
            for j in dir():
                if re.fullmatch(r'^script1_text\d{2}$', j):
                    script1_list.append(j)
                if re.fullmatch(r'^script2_text\d{2}$', j):
                    script2_list.append(j)
                if re.fullmatch(r'^script3_text\d{2}$', j):
                    script3_list.append(j)

            # script1_list_sorted = sorted(script1_list, reverse=True)
            script1_list_sorted = sorted(script1_list)
            script1_list_concat = '+'.join(script1_list_sorted)
            script1_create = eval(script1_list_concat)

            script2_list_sorted = sorted(script2_list)
            script2_list_concat = '+'.join(script2_list_sorted)
            script2_set = eval(script2_list_concat)

            script3_list_sorted = sorted(script3_list)
            script3_list_concat = '+'.join(script3_list_sorted)
            script3_update = eval(script3_list_concat)

            self.sqlite_cursor.execute(script0_drop)
            self.sqlite_cursor.execute(script1_create)
            self.sqlite_cursor.execute(script2_set)
            self.sqlite_cursor.execute(script3_update)

            # print('='*50)
            # print(script0_drop)
            # print('-' * 50)
            # print(script1_create)
            # print('-' * 50)
            # print(script2_set)
            # print('-' * 50)
            # print(script3_update)

        self.sqlite_cursor.execute('COMMIT')
        # self.output_log += "-" * 40 + "\n"

    @staticmethod
    def compare_string(a_val: str, b_val: str, perf_flag: int = 1) -> int:
        """
        Compare two strings.

        Parameters
        ----------
        a_val : str
            First string to compare.
        b_val : str
            Second string to compare.
        perf_flag : int, optional
            Flag to show whether to use performance optimization. The default is 1.

        Returns
        -------
        int
            0 if strings are not similar, 1 if strings are similar.
        """
        if perf_flag == 1:
            return 0

        def regexp_replace(s: str, pattern: str, repl: str) -> str:
            return re.sub(pattern, repl, s)

        def regexp_match(s: str, pattern: str) -> bool:
            return re.fullmatch(pattern, s) is not None

        a_str = str(int("0" + regexp_replace(a_val, r"\D", "")))
        b_str = str(int("0" + regexp_replace(b_val, r"\D", "")))

        a_sum = sum(int(d) for d in re.findall(r"\d", a_str))
        b_sum = sum(int(d) for d in re.findall(r"\d", b_str))

        a_len = len(a_str)
        b_len = len(b_str)

        if a_len == b_len:
            if a_sum == b_sum:
                j = ""
                aa = ""
                bb = ""
                for i in range(a_len):
                    if a_str[i] == b_str[i]:
                        j += "0"
                    else:
                        j += "1"
                        aa += a_str[i]
                        bb = b_str[i] + bb
                j_clean = re.sub(r"0*", "", j)
                if j_clean == "11" and aa == bb:
                    return 1
            else:
                j = ""
                for i in range(a_len):
                    j += "0" if a_str[i] == b_str[i] else "1"
                j_clean = re.sub(r"0*", "", j)
                if j_clean == "1":
                    return 1
        else:
            if 1 <= (a_len - b_len) <= 2 and (
                regexp_match(a_str, r"^\d{1,2}" + re.escape(b_str) + r"$") or
                regexp_match(a_str, r"^" + re.escape(b_str) + r"\d{1,2}$") or
                regexp_match(a_str, r"^\d" + re.escape(b_str) + r"\d$")
            ):
                return 1
            elif 1 <= (b_len - a_len) <= 2 and (
                regexp_match(b_str, r"^\d{1,2}" + re.escape(a_str) + r"$") or
                regexp_match(b_str, r"^" + re.escape(a_str) + r"\d{1,2}$") or
                regexp_match(b_str, r"^\d" + re.escape(a_str) + r"\d$")
            ):
                return 1
            elif (a_len - b_len) == 1:
                b_str1 = regexp_replace(b_str, r"(\d(?!$))", r"\1.*")
                if regexp_match(a_str, r"^" + b_str1 + r"$"):
                    return 1
            elif (b_len - a_len) == 1:
                a_str1 = regexp_replace(a_str, r"(\d(?!$))", r"\1.*")
                if regexp_match(b_str, r"^" + a_str1 + r"$"):
                    return 1
        return 0

    # Обёртка для SQLite (без perf_flag, по умолчанию True)
    def sqlite_compare_string(self, a, b, perf_flag):
        """
        SQLite wrapper for compare_string.

        Parameters
        ----------
        a : str
            First string to compare.
        b : str
            Second string to compare.
        perf_flag : int
            Flag to show whether to use performance optimization.

        Returns
        -------
        result : int
            0 if strings are not similar, 1 if strings are similar.

        """
        return self.compare_string(str(a), str(b), perf_flag)

    def sqlite_analysis_process2(self):
        self.sqlite_conn.create_function("compare_string", 3, self.sqlite_compare_string)

        # t-est!
        # a = '123456'
        # b = '123465'
        # perf_flag = 1
        # self.sqlite_cursor.execute(f"SELECT compare_string('{a}', '{b}', {perf_flag})")
        # rows = self.sqlite_cursor.fetchall()
        # return rows[0][0]

        for key, value in self.equipment.items():
            script4_drop = f"drop table if exists tmp_{key}_analysis;"

            script5_text01 = f"""create table tmp_{key}_analysis as
SELECT r2.order_number,\n"""

            script5_text02 = ''

            script5_text03 =  """  FROM (SELECT order_number,
               carriage_code,
               repair_number,
               max_repair_number,\n"""

            script5_text04 = ''

            script5_text05 = ''
            script5_text06 = ''

            script5_text07 = """          FROM (SELECT rr.order_number,
                       rr.carriage_code,
                       rr.repair_number,
                       max_repair_number,\n"""

            script5_text08 = ''
            script5_text09 = ''

            script5_text10 = f"""                          FROM (((select *
                                    from repairs
                                   where carriage_code in
                                         (SELECT distinct carriage_code
                                            FROM repairs
                                           where ({value[0]}_err is null)
                                              or not
                                                  ((SELECT value FROM settings where key = 'full_analysis')='0'))) AS rr LEFT JOIN
                                repairs AS rn
                                ON(rr.repair_number = rn.repair_number - 1) AND
                                (rr.carriage_code = rn.carriage_code)) LEFT JOIN
                                repairs AS rp
                                ON(rr.carriage_code = rp.carriage_code) AND
                                (rr.repair_number = rp.repair_number + 1))
                          left join (select rrm.carriage_code,
                                           max(rrm.repair_number) as max_repair_number
                                      from (select *
                                    from repairs
                                   where carriage_code in
                                         (SELECT distinct carriage_code
                                            FROM repairs
                                           where ({value[0]}_err is null)
                                              or not
                                                  ((SELECT value FROM settings where key = 'full_analysis')='0'))) as rrm
                                     group by carriage_code) as rm
                            on (rr.carriage_code = rm.carriage_code)
                         ORDER BY rr.carriage_code, rr.repair_number) as r1) as r2;"""

            script6_text01 = 'UPDATE repairs SET\n'

            script6_text02 = ''

            script6_text03 = f""" FROM tmp_{key}_analysis AS t
 WHERE repairs.order_number = t.order_number;"""

            for j in value:
                for x in ['p', 'n']:
                    for i in value:
                        script5_text02_value = f"||'|'||{x}1_".join(value)
                        script5_text02 += f"       compare_string(r_{j}, {x}1_{i}, iif(regexp_like(r_{j},'^('||{x}1_{script5_text02_value}||')$'),1,0)) +\n"
                script5_text02 = re.sub(r'\+\n$', f'AS {j}_err,', script5_text02) + "\n"

                script5_text05 += f"               IIf(rp_{key} Is Null And repair_number = 1, r_{j}, p_{j}) AS p1_{j},\n"
                script5_text06 += f"               IIf(rn_{key} Is Null And repair_number = max_repair_number, r_{j}, n_{j}) AS n1_{j},\n"
                script6_text02 += f"       {j}_err = t.{j}_err,\n"

            for x in ['r', 'p', 'n']:
                for i in value:
                    script5_text04 += f"               {x}_{i},\n"
                    script5_text08 += f"                       iif(r{x}.{i} is null, '{x}', r{x}.{i}) AS {x}_{i},\n"

            for x in ['p', 'n']:
                script5_text09_value = f" ||r{x}.".join(value)
                script5_text09 += f"                       r{x}.{script5_text09_value} AS r{x}_{key},\n"

            script5_text02 = re.sub(r',\n$', '', script5_text02) + "\n"
            script5_text06 = re.sub(r',\n$', '', script5_text06) + "\n"
            script5_text09 = re.sub(r',\n$', '', script5_text09) + "\n"
            script6_text02 = re.sub(r',\n$', '', script6_text02) + "\n"

            script5_list = []
            script6_list = []
            for j in dir():
                if re.fullmatch(r'^script5_text\d{2}$', j):
                    script5_list.append(j)
                if re.fullmatch(r'^script6_text\d{2}$', j):
                    script6_list.append(j)

            # script5_list_sorted = sorted(script5_list, reverse=True)
            script5_list_sorted = sorted(script5_list)
            script5_list_concat = '+'.join(script5_list_sorted)
            script5_create = eval(script5_list_concat)

            script6_list_sorted = sorted(script6_list)
            script6_list_concat = '+'.join(script6_list_sorted)
            script6_set = eval(script6_list_concat)

            self.sqlite_cursor.execute(script4_drop)
            self.sqlite_cursor.execute(script5_create)
            self.sqlite_cursor.execute(script6_set)

            # print('='*50)
            # print(script4_drop)
            # print('-' * 50)
            # print(script5_create)
            # print('-' * 50)
            # print(script6_set)

        self.sqlite_cursor.execute('COMMIT')
