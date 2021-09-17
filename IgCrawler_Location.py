# Instagram Location爬蟲

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


def insert_posts(cur, tar, gos):
    try:
        cur.execute("""
        INSERT INTO [dbo].[Posts] (PostId, LocationId, IGUserId, PostDate, Photolikes, Photocomments, PhotoUrl, DownloadUrl, ShortCode) 
        VALUES(?, ?, ?, ?, ?, ?, ?, '', ?)
        """, tar["node"]["id"], gos.LocationId, tar["node"]["owner"]["id"],
                    datetime.utcfromtimestamp(tar["node"]["taken_at_timestamp"]).strftime("%Y-%m-%d"),
                    tar["node"]["edge_media_preview_like"]["count"], tar["node"]["edge_media_to_comment"]["count"],
                    tar["node"]["display_url"], tar["node"]["shortcode"])
    except Exception as ex:
        dmUtility.write_error_log(ex.__context__)
        cur.execute("UPDATE [dbo].[LocationLog] SET ErrorMessage = ? WHERE LocationId = ?", "INSERT_POSTS有誤",
                    gos.LocationId)
        dmUtility.dispose_browser(browser)


def insert_posts_tag(cur, tar, gos):
    cur.execute("INSERT INTO [dbo].[PostTags] (PostId, TagName) VALUES(?, ?)", tar["node"]["id"], gos.TagName)


chrome_options = None
browser = None

try:
    sqlConn = pyodbc.connect(dmConfig.db_connect, autocommit=True)
    cursor = sqlConn.cursor()

    cursor.execute("""
        SELECT [LocationId]
              ,[TagName]
              ,[ThreadId]
              ,[FinishedTime]
              ,[ErrorMessage]
              ,[ShortCode]
        FROM [FollowIGtoTravelDb].[dbo].[LocationLog]
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
        # 海外地點
        out_of_sea_count = 0
        # 如果都有登入的話才開始請求
        for row in rows:
            try:
                now = datetime.now()
                if now.hour % 4 == 0 and now.minute < 10:
                    print('0 4 8 12 16 20 24 時 0 ~ 10 分 配合 ChromeKiller 結束程式')
                    break
                json_content = get_body(f'https://www.instagram.com/explore/locations/{row.LocationId}?__a=1')
                time.sleep(2)
                # 通常不會有 因為已經將空白LocationId排除，不過為了避免有其他無法預測到的標籤所以還是紀錄LOG
                if json_content == '{}':
                    cursor.execute("UPDATE [dbo].[LocationLog] SET ErrorMessage = ? WHERE LocationId = ?",
                                   "很抱歉，此頁面無法使用。", row.LocationId)
                    dmUtility.write_error_log('很抱歉，此頁面無法使用。')
                    no_tag_count += 1
                    continue
                shared_data = None
                json_data = None

                try:
                    shared_data = json.loads(json_content)
                    if "graphql" in shared_data:
                        json_data = shared_data["graphql"]["location"]
                    elif "status" in shared_data:
                        dmUtility.send_line_notify(f"LocationId:{row.LocationId},分流{args.thread_id}：帳號出現異常，請注意。")
                        fail_count += 1
                        break
                except Exception as e:
                    dmUtility.write_error_log(e.__context__)
                    cursor.execute("UPDATE [dbo].[LocationLog] SET ErrorMessage = ? WHERE LocationId = ?",
                                   "取得json_data有誤，或是json_data為空", row.LocationId)
                    dmUtility.send_line_notify(f"分流：{args.thread_id}，{row.LocationId} 該LocationId取得json_data有誤")
                    fail_count += 1
                    break

                # 排除lat空值
                if json_data["lat"] is not None:
                    lat = json_data["lat"]
                    lng = json_data["lng"]
                    # 最大長度50
                    location_name = json_data["name"][:50]
                    # 熱門貼文
                    top_post = json_data["edge_location_to_top_posts"]["edges"]
                    # 最新貼文
                    last_post = json_data["edge_location_to_media"]["edges"]
                    # 排除海外地點
                    if lat > 26.436 or lat < 21.8:
                        cursor.execute(
                            "UPDATE [dbo].[LocationLog] SET ErrorMessage = ? WHERE LocationId = ?", "海外地點",
                            row.LocationId)
                        out_of_sea_count += 1
                        continue
                    if lng > 122.2 or lng < 118.18:
                        cursor.execute(
                            "UPDATE [dbo].[LocationLog] SET ErrorMessage = ? WHERE LocationId = ?", "海外地點",
                            row.LocationId)
                        out_of_sea_count += 1
                        continue
                    # 該筆資料新增進Location

                    cursor.execute(
                        "INSERT INTO [dbo].[Location] (LocationId, LocationName, lat, lng) VALUES(?, ?, ?, ?)",
                        row.LocationId, location_name, lat, lng)
                    # 該筆資料的熱門貼文新增進POSTS, POSTS_TAG
                    for top in top_post:
                        shortCode = top["node"]["shortcode"]
                        cursor.execute("SELECT ShortCode FROM Posts WHERE ShortCode = ? ", shortCode)
                        post = cursor.fetchone()
                        if post is None:
                            # INSERT POSTS
                            insert_posts(cursor, top, row)
                            # INSERT POSTS_TAG
                            insert_posts_tag(cursor, top, row)
                    # 該筆資料的最新貼文新增進POSTS, POSTS_TAG
                    for last in last_post:
                        shortCode = last["node"]["shortcode"]
                        cursor.execute("SELECT ShortCode FROM Posts WHERE ShortCode = ? ", shortCode)
                        post = cursor.fetchone()
                        if post is None:
                            # INSERT POSTS
                            insert_posts(cursor, last, row)
                            # INSERT POSTS_TAG
                            insert_posts_tag(cursor, last, row)
                # 每一輪LocationId正常結束之後 才會寫入DB
                cursor.execute("UPDATE [dbo].[LocationLog] SET FinishedTime = ? WHERE LocationId = ?",
                               datetime.now().strftime("%Y/%m/%d %H:%M:%S.%f")[:-3], row.LocationId)

                time.sleep(random.randint(20, 30))
                success_count += 1

            except Exception as e:
                dmUtility.write_error_log(f'{row.LocationId} : {e.__context__}')
                cursor.execute("UPDATE [dbo].[LocationLog] SET ErrorMessage = ? WHERE TagName = ?", e.__context__,
                               row.LocationId)
                fail_count += 1
                exit(0)

        dmUtility.send_line_notify(
            f'{date.today().strftime("%Y/%m/%d")}\n地標(Location)分流：{args.thread_id}\n總共：{success_count + no_tag_count + fail_count + out_of_sea_count}筆\n成功：{success_count}筆\n無人使用：{no_tag_count}筆\n失敗：{fail_count}筆\n海外：{out_of_sea_count}筆',
            2)

except Exception as e:
    dmUtility.write_error_log(e.__context__)
finally:
    dmUtility.dispose_browser(browser)
    exit(0)
