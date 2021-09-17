# Instagram ShortCode爬蟲

import argparse
import json
import time
from datetime import datetime
from datetime import date
import random

import pyodbc

from dotmore import dmConfig
from dotmore import dmUtility

parser = argparse.ArgumentParser()
parser.add_argument('--thread_id', help='要運行的 Thread 編號')
parser.add_argument('--profile_name', help='要運行的 chrome profile 名稱')
args = parser.parse_args()

# 判斷執行時有沒有給予參數 沒有給予參數報錯並跳出
if args.thread_id is None:
    print('缺少參數 thread_id')
    exit(0)

# 判斷執行時有沒有給予參數 沒有給予參數報錯並跳出
if args.profile_name is None:
    print('缺少參數 profile_name')
    exit(0)

dmUtility.init_log(args.thread_id)
sleep_second = 13


def get_body(ig_url):
    text = browser.execute_script("""
function httpGet(theUrl)
{
    var xmlHttp = new XMLHttpRequest();
    xmlHttp.open( "GET", theUrl, false ); // false for synchronous request
    xmlHttp.send( null );
    return xmlHttp.responseText;
}
return httpGet('""" + ig_url.replace('\\', '\\\\') + "');")
    return text


chrome_options = None
browser = None

try:
    sqlConn = pyodbc.connect(dmConfig.db_connect, autocommit=True)
    cursor = sqlConn.cursor()

    cursor.execute("""
        SELECT [ShortCode]
              ,[TagName]
              ,[ThreadId]
              ,[FinishedTime]
              ,[ErrorMessage]
              ,[RecordDate]
        FROM [FollowIGtoTravelDb].[dbo].[ShortCodeLog]
        WHERE ThreadId = ?
        AND FinishedTime IS NULL
        AND ErrorMessage IS NULL
        ORDER BY [TagName]
    """, args.thread_id)
    rows = cursor.fetchall()

    if len(rows) > 0:
        chrome_options = dmUtility.get_chrome_option(2, args.profile_name)
        browser = dmUtility.initial_browser(chrome_options, 30, 10)
        # 強制延遲
        time.sleep(3)
        browser.get(f'https://www.instagram.com/')
        time.sleep(7)
        # 檢查是否有登入
        is_login = browser.execute_script("""
            var q = document.querySelector('input[aria-label=\"手機號碼、用戶名稱或電子郵件地址\"]');
            if (q == undefined)
                return 1;
            else
                return 0;
        """)

        if is_login == 0:
            dmUtility.dispose_browser(browser)
            dmUtility.send_line_notify(f'IG 數據爬蟲: {args.thread_number} Profile: {args.profile_name} 使用者已被登出')
            exit(0)

        if browser.page_source.find("確認身分以登入") != -1:
            dmUtility.dispose_browser(browser)
            dmUtility.send_line_notify(f'IG 數據爬蟲: {args.thread_number} Profile: {args.profile_name} 帳號異常活動，需確認身份')
            exit(0)
        elif browser.page_source.find("輸入新的電子郵件地址") != -1:
            dmUtility.dispose_browser(browser)
            dmUtility.send_line_notify(f'IG 數據爬蟲: {args.thread_number} Profile: {args.profile_name} 輸入新的電子郵件地址')
            exit(0)
        elif browser.page_source.find("協助我們確認此帳號為你所有") != -1:
            dmUtility.dispose_browser(browser)
            dmUtility.send_line_notify(f'IG 數據爬蟲: {args.thread_number} Profile: {args.profile_name} 協助我們確認此帳號為你所有')
            exit(0)

        # 成功筆數
        success_count = 0
        # 失敗筆數
        fail_count = 0
        # 無人使用筆數
        no_tag_count = 0
        # 如果都有登入的話才開始請求
        for row in rows:
            try:
                now = datetime.now()
                if now.hour % 4 == 0 and now.minute < 10:
                    print('0 4 8 12 16 20 24 時 0 ~ 10 分 配合 ChromeKiller 結束程式')
                    break
                json_content = get_body(f'https://www.instagram.com/p/{row.ShortCode}?__a=1')
                time.sleep(2)
                # 通常不會有 因為已經將空白ShortCode排除，不過為了避免有其他無法預測到的標籤所以還是紀錄LOG
                if json_content == '{}':
                    cursor.execute("UPDATE [dbo].[ShortCodeLog] SET ErrorMessage = ? WHERE ShortCode = ?",
                                   "你點擊的連結可能發生故障，或該頁面已遭移除。", row.ShortCode)
                    dmUtility.write_error_log(f'{row.ShortCode}該連結可能發生故障，或該頁面已遭移除。')
                    no_tag_count += 1
                    continue
                shared_data = None
                json_data = None

                try:
                    shared_data = json.loads(json_content)
                    # 判斷帳號是不是需要驗證
                    if "graphql" in shared_data:
                        json_data = shared_data["graphql"]["shortcode_media"]
                    elif "status" in shared_data:
                        dmUtility.send_line_notify(f"ShortCode:{row.ShortCode},分流{args.thread_id}：帳號出現異常，請注意。")
                        fail_count += 1
                        break
                except Exception as e:
                    dmUtility.write_error_log(e.__context__)
                    cursor.execute("UPDATE [dbo].[ShortCodeLog] SET ErrorMessage = ? WHERE ShortCode = ?",
                                   "取得json_data有誤", row.ShortCode)
                    dmUtility.send_line_notify(f"分流：{args.thread_id}，{row.ShortCode} 該ShortCode取得json_data有誤")
                    fail_count += 1
                    break
                except ValueError as ve:
                    dmUtility.write_error_log(ve)
                    cursor.execute("UPDATE [dbo].[ShortCodeLog] SET ErrorMessage = ? WHERE ShortCode = ?",
                                   "取得json_data有誤2", row.ShortCode)
                    dmUtility.send_line_notify(f"分流：{args.thread_id}，{row.ShortCode} 該ShortCode取得json_data有誤")
                    break

                locations = json_data["location"]

                if locations is not None and locations["id"] is not None:
                    # 判斷Location曾經有沒有抓過這個location_id
                    cursor.execute(
                        "SELECT LocationId FROM [FollowIGtoTravelDb].[dbo].[Location] WHERE LocationId = ? ",
                        locations["id"])
                    post = cursor.fetchone()
                    # 判斷locationLog曾經有沒有抓過這個location_id
                    if post is None:
                        cursor.execute("SELECT LocationId FROM LocationLog WHERE LocationId = ? ", locations["id"])
                        post = cursor.fetchone()
                    else:
                        cursor.execute(
                            "UPDATE [dbo].[LocationLog] SET [ThreadId] = Null, FinishedTime = Null WHERE LocationId = ? ",
                            locations["id"])
                    # 如果都沒有抓過的話才取這個ShortCode
                    if post is None:
                        cursor.execute(
                            "INSERT INTO LocationLog (LocationId, TagName, RecordDate, ShortCode) VALUES (?, ?, ?, ?)",
                            locations["id"], row.TagName, date.today(), row.ShortCode)

                    # 每一輪ShortCode正常結束之後 才會寫入DB
                    cursor.execute("UPDATE [dbo].[ShortCodeLog] SET FinishedTime = ? WHERE ShortCode = ?",
                                   datetime.now().strftime("%Y/%m/%d %H:%M:%S.%f")[:-3], row.ShortCode)

                else:
                    cursor.execute(
                        "UPDATE [dbo].[ShortCodeLog] SET ErrorMessage = 'Location is Null' WHERE ShortCode = ?",
                        row.ShortCode)

                time.sleep(random.randint(20, 30))
                success_count += 1

            except Exception as e:
                dmUtility.write_error_log(f'{row.ShortCode} : {e.__context__}')
                cursor.execute("UPDATE [dbo].[ShortCodeLog] SET ErrorMessage = ? WHERE ShortCode = ?", e.__context__,
                               row.ShortCode)
                fail_count += 1
                exit(0)

        dmUtility.send_line_notify(
            f'{date.today().strftime("%Y/%m/%d")}\n短文章(ShortCode)分流：{args.thread_id}\n總共：{success_count + no_tag_count + fail_count}筆\n成功：{success_count}筆\n無人使用：{no_tag_count}筆\n失敗：{fail_count}筆',
            2)

except Exception as e:
    dmUtility.write_error_log(e.__context__)
finally:
    dmUtility.dispose_browser(browser)
    exit(0)
