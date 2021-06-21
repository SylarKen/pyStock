import datetime
import time

import baostock as bs
import pandas as pd
import pymysql


def get_all_stock(wr="r"):
    date = datetime.date.today() - datetime.timedelta(days=1)
    print(date)
    lg = bs.login()
    rs = bs.query_all_stock(day=date)
    data_list = []
    while (rs.error_code == '0') & rs.next():
        data_list.append(rs.get_row_data())
    result = pd.DataFrame(data_list, columns=rs.fields)
    # csv = result.to_csv("all_stock.csv", encoding="gbk", index=False)
    bs.logout()
    db = pymysql.connect(host="106.13.166.168", port=3306, user="root", passwd="sylarken_851107", database="db_stock",
                         charset='utf8', cursorclass=pymysql.cursors.DictCursor)
    # db = pymysql.connect(host="139.224.197.155", port=3306, user="sylar", passwd="Sylar@851107", database="db_stock",
    #                      charset='utf8', cursorclass=pymysql.cursors.DictCursor)
    # db = pymysql.connect(host="127.0.0.1", port=3306, user="sylar", passwd="Sylar@851107", database="db_stock",
    #                      charset='utf8', cursorclass=pymysql.cursors.DictCursor)
    cursor = db.cursor()
    cursor.execute("SELECT MAX(code) as max_issue FROM `t_stock`")
    result_max = cursor.fetchone()
    print(result_max)
    print(result.shape[0])
    print(result.loc[0]['code'])

    if wr == "w":
        # 写入数据库
        sql = ""
        # for i in range(result.shape[0]):
        #     name = str(result.loc[i]['code_name'])
        #     code = str(result.loc[i]['code'])
        #     marketCode = code.split('.')[0]
        #     tradeStatus = str(result.loc[i]['tradeStatus'])
        #     sql_update = "UPDATE `t_stock` SET `name` = '{1}', `marketCode` = '{2}', `tradeStatus` = '{3}', `time_edit` = NOW() WHERE `code` = '{0}'".format(
        #         code, name, marketCode, tradeStatus)
        #     sql_insert = "INSERT INTO `t_stock`(`code`, `marketCode`, `name`, `tradeStatus`, `time_create`, `time_edit`, `u1`, `u2`, `u3`) VALUES ('{0}', '{1}', '{2}', '{3}', NOW(), NOW(),  NULL, NULL, NULL);".format(
        #         code, marketCode, name, tradeStatus)
        #     try:
        #         result_update = cursor.execute(sql_update)
        #         print(result_update)
        #         if result_update == 0:
        #             cursor.execute(sql_insert)
        #         db.commit()
        #     except Exception as e:
        #         db.rollback()
        #         print(str(e))
        # for i in range(result.shape[0]):
        #     name = str(result.loc[i]['code_name'])
        #     code = str(result.loc[i]['code'])
        #     marketCode = code.split('.')[0]
        #     tradeStatus = str(result.loc[i]['tradeStatus'])
        #     sql = sql + "INSERT INTO `t_stock` (`code`, `marketCode`, `name`, `tradeStatus`, `time_create`, `time_edit`, `u1`, `u2`, `u3`) VALUES ('{0}', '{1}', '{2}', '{3}', NOW(), NOW(),  NULL, NULL, NULL) ON	DUPLICATE KEY UPDATE `marketCode` = '{1}', `name` = '{2}', `tradeStatus` = '{3}', `time_edit` = NOW(); \t\n".format(
        #         code, marketCode, name, tradeStatus)
        #     try:
        #         result_update = cursor.executemany(sql)
        #         print(result_update)
        #         if result_update == 0:
        #             cursor.execute(sql)
        #         db.commit()
        #     except Exception as e:
        #         db.rollback()
        #         print(str(e))
        start = time.time()
        sql = "INSERT INTO `t_stock` (`code`, `marketCode`, `name`, `tradeStatus`, `time_create`, `time_edit`, `u1`, `u2`, `u3`) VALUES (%s, %s, %s, %s, NOW(), NOW(),  NULL, NULL, NULL) ON	DUPLICATE KEY UPDATE `marketCode` = %s, `name` = %s, `tradeStatus` = %s, `time_edit` = NOW(); "
        # sql = "INSERT INTO `t_stock` (`code`, `marketCode`, `name`, `tradeStatus`, `time_create`, `time_edit`, `u1`, `u2`, `u3`) VALUES (%s, %s, %s, %s, NOW(), NOW(),  NULL, NULL, NULL) ; "
        args = []
        len = result.shape[0]
        for i in range(len):
            row = result.loc[i]
            name = str(row['code_name'])
            code = str(row['code'])
            marketCode = code.split('.')[0]
            tradeStatus = str(row['tradeStatus'])
            args.append((code, marketCode, name, tradeStatus, marketCode, name, tradeStatus))
            # args.append((code, marketCode, name, tradeStatus))
        print('共计用时（秒）：' + str(round(time.time() - start, 2)))
        start = time.time()
        try:
            result_update = cursor.executemany(sql, tuple(args))
            print(result_update)
            if result_update == 0:
                cursor.execute(sql)
            db.commit()
        except Exception as e:
            db.rollback()
            print(str(e))
        print('共计用时（秒）：' + str(round(time.time() - start, 2)))

if __name__ == '__main__':
    get_all_stock("w")
