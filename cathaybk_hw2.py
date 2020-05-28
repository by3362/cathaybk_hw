import asyncio
from pyppeteer import launch
import requests
from urllib3.exceptions import InsecureRequestWarning
from bs4 import BeautifulSoup
import re
from PIL import Image
import pytesseract
import time
import pymongo
from fake_useragent import UserAgent
import random
import ssl
import socket 
import urllib3

def convert_img(img,threshold):
    # 把圖片放大方便辨識
    basewidth = 180
    wpercent = (basewidth/float(img.size[0]))
    hsize = int((float(img.size[1])*float(wpercent)))
    img = img.resize((basewidth,hsize), Image.ANTIALIAS)
    img = img.convert("L") # 處理灰度 
    pixels = img.load() 
    for x in range(img.width): 
        for y in range(img.height): 
            if pixels[x, y] > threshold: 
                pixels[x, y] = 255 
            else: 
                pixels[x, y] = 0 
    return img

async def main(url):
    ua = UserAgent()
    browser = await launch({'headless': True,             # 是否啟用 Headless 模式
                            'devtools': False,             # 否為每一個頁面自動開啟調試工具，默認是 False。如果這個參數設置為 True，那麼 headless 參數就會無效，會被強制設置為 False。
                            'args': [ 
                                '--disable-extensions',
                                '--disable-infobars', # 關閉自動軟體提示info bar
                                '--hide-scrollbars',  # 隱藏屏幕截圖中的滾動條
                                '--disable-bundled-ppapi-flash', # 禁用捆綁的PPAPI版本的Flash
                                '--mute-audio',  # 使發送到音頻設備的音頻靜音，以便在自動測試過程中聽不到聲音
                                '--no-sandbox',           # --no-sandbox 在 docker 里使用时需要加入的参数，不然会报错 (禁用所有通常沙盒化的進程類型的沙盒)
                                '--disable-setuid-sandbox', # Disable the setuid sandbox (Linux only)
                                '--disable-gpu', # 禁用GPU硬件加速。如果沒有軟件渲染器，則GPU進程將不會啟動
                                '--disable-xss-auditor',
                                '--suppress-message-center-popups', # 隱藏所有消息中心通知彈出窗口。用於測試。
                            ],
                            'dumpio': True,               # 解决浏览器多开卡死
                        })
    page = await browser.newPage()
    # await page.setUserAgent('Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/81.0.4044.138 Safari/537.36')
    await page.setUserAgent(ua.random)
	# 第二步，修改 navigator.webdriver检测
	# 其实各种网站的检测js是不一样的，这是比较通用的。有的网站会检测运行的电脑运行系统，cpu核心数量，鼠标运行轨迹等等。
    # 反爬js
    js_text = """
            () =>{ 
                Object.defineProperties(navigator,{ webdriver:{ get: () => false } });
                window.navigator.chrome = { runtime: {},  };
                Object.defineProperty(navigator, 'languages', { get: () => ['en-US', 'en'] });
                Object.defineProperty(navigator, 'plugins', { get: () => [1, 2, 3, 4, 5,6], });
            }
                """
    await page.evaluateOnNewDocument(js_text)  # 本页刷新后值不变，自动执行js
    res = await page.goto(url, options={'waitUntil': 'networkidle0'})

    ## 關閉 選擇地區 的 彈出視窗，否則 click 下一頁是無效的
    await page.click('a.area-box-close')
    page.mouse
    await asyncio.sleep(int(random.uniform(3, 5)))
    # await page.waitFor(int(random.uniform(3, 7))*1000)
    # await page.click('a.close')
    # page.mouse
    
    data = await page.content()
    soup = BeautifulSoup(data, "html5lib")
    totalpages = int(int(soup.select('div.pageBar > span.TotalRecord')[0].text.split(" ")[1])/30)+1
    counter = 0

    client = pymongo.MongoClient('localhost', 27017)
    mydb = client['mongodb']
    rentdb = mydb['rentdb']

    #這裡對整個socket層設定超時時間
    socket.setdefaulttimeout(20)

    for i in range(totalpages):
        start = time.time()
        data = await page.content()
        soup = BeautifulSoup(data, "html5lib")
        links = soup.select("div#content > ul.listInfo > li.pull-left.infoContent")
        # headers = {
        # 'user-agent': ua.random
        # }
        info_list = []
        # Link迴圈
        for link in links:
            headers = {
                'user-agent': ua.random
            }
            #### 連結
            link = "https:" + link.h3.a["href"].strip()
            # verify 新增內建 SLL認證書 及 timeout
            # 超時重試 5 次
            times = 0
            while times <= 5:
                try:
                    requests.packages.urllib3.disable_warnings(category=InsecureRequestWarning)
                    link_data = requests.get(url=link, headers=headers, verify = False, timeout = (10,15))
                    link_soup = BeautifulSoup(link_data.text, 'html5lib')
                    # print(link)
                    ## 抓取title 跳出index error
                    try:
                        title = link_soup.select_one("div.houseInfo > h1 > span.houseInfoTitle").text.strip()
                    except IndexError as e:
                        print(e, " , Error in Title，link : ", link)
                        title = ""
                        print(link_soup)
                    except AttributeError as e:
                        print(e, " , Error in Title，link : ", link)
                        title = ""
                        print(link_soup)
                    userInfo = link_soup.find("div", "userInfo")
                    # 有些userinfo 會有複數 div
                    try:
                        owner = userInfo.select_one("div.infoOne > div.avatarRight > div").text.strip()
                        # 問題 : 分隔符號有分全形跟半形
                        Delimiter_brackets = ""
                        if "（" in owner:
                            Delimiter_brackets = "（"
                        elif "(" in owner:
                            Delimiter_brackets = "("
                        #### 出租者
                        landlord = owner.split(Delimiter_brackets)[0]
                        #### 出租者身分
                        identity = Delimiter_brackets + owner.split(Delimiter_brackets)[1]
                    except AttributeError as e:
                        print(e, " , Error in Owner，link : ", link)
                        landlord = ""
                        identity = ""

                    # img 有 text 或 圖片
                    #### 連絡電話
                    tel_num = ""
                    tel_src = userInfo.select("span.num")
                    times2 = 0
                    while times2 <= 5:
                        try:
                            if tel_src[0].img:
                                tel_src = tel_src[0].img["src"].strip()
                                res = requests.get(url="http:" + tel_src + ".jpg", headers=headers, verify = False, timeout = (10,15))
                                with open("temp.jpg", "wb") as f:
                                    f.write(res.content)
                                img = Image.open("temp.jpg")
                                # 遇見問題 1 : 文字完整卻無法辨識，故推斷可能是圖片太小，修改程式碼使圖片放大，檢視結果皆正確
                                # 整理灰度 並 二值化 (使圖片更好辨識)
                                img = convert_img(img, 150)
                                # Show 出圖片檢視
                                # img.show()
                                tel_num = pytesseract.image_to_string(img).replace(" ","")
                            else:
                                tel_num = tel_src[0].text.strip()
                            
                            # 若tel_src有值，則跳出迴圈
                            if tel_num != "":
                                break
                        except requests.exceptions.ConnectTimeout:
                            times2 += 1
                            print("Requests Timeout Error, link : http:", tel_src, ".jpg, 重試第", times, "次")
                            await asyncio.sleep(int(random.uniform(10, 15)))
                        except requests.exceptions.SSLError:
                            times2 += 1
                            print("Requests SSL Error, link : http:", tel_src, ".jpg, 重試第", times, "次")
                            await asyncio.sleep(int(random.uniform(10, 15)))
                        except requests.exceptions.ReadTimeout:
                            times2 += 1
                            print("Requests Read Timeout Error, link : http:", tel_src, ".jpg, 重試第", times, "次")
                            await asyncio.sleep(int(random.uniform(10, 15)))
                        except requests.exceptions.ConnectionError as e:
                            times2 += 1
                            print(e, ", link : http:", tel_src, ".jpg, 重試第", times, "次")
                            await asyncio.sleep(int(random.uniform(50, 60)))
                        except ssl.SSLError:
                            times2 += 1
                            print("SSL Error, link : http:", tel_src, ".jpg, 重試第", times, "次")
                            await asyncio.sleep(int(random.uniform(10, 15)))
                        except urllib3.exceptions.ProtocolError:
                            times2 += 1
                            print("urlib3 Protocol Error, link : http:", tel_src, ".jpg, 重試第", times, "次")
                            await asyncio.sleep(int(random.uniform(10, 15)))
                    if times2 == 5:
                        print("http:" , tel_src, ".jpg, 連線五次皆失敗!故跳過 !")
                        tel_num = ""


                    #### 租金
                    detailInfo = link_soup.find("div", "detailInfo")
                    price = detailInfo.select("div.price > i")[0].text

                    #### 型態housetype，現況situation，坪數pings，樓層floor，格局pattern，
                    attrs = detailInfo.select("ul.attr > li")
                    housetype = ""
                    situation = ""
                    pings = ""
                    pattern = ""
                    for attr in attrs:
                        attr_text = attr.text.strip().replace(" ", "")
                        temp_word = ""
                        if "型態" in attr_text:
                            housetype = attr_text.split(":")[1].replace(u"\xa0", u"")
                        elif "現況" in attr_text:
                            situation = attr_text.split(":")[1].replace(u"\xa0", u"")
                        elif "坪數" in attr_text:
                            pings = attr_text.split(":")[1].replace(u"\xa0", u"")
                        elif "樓層" in attr_text:
                            floor = ""
                            floor = attr_text.split(":")[1].replace(u"\xa0", u"")
                        elif "格局" in attr_text:
                            pattern = attr_text.split(":")[1].replace(u"\xa0", u"")
                    
                    #### 入住者需求 : 性別要求genderrequire，押金deposit，最短租期
                    ResidentNeeds = link_soup.select("ul.labelList.labelList-1 > li")
                    genderrequire = ""
                    deposit = ""
                    minlease = ""
                    for rns in ResidentNeeds:
                        temp_word = ""
                        rns_text = rns.text.strip().replace(" ", "")
                        if "性別要求" in rns_text:
                            genderrequire = rns_text.split("：")[1].replace(u"\xa0", u"")
                        elif "押金" in rns_text:
                            deposit = rns_text.split("：")[1].replace(u"\xa0", u"")
                        elif "最短租期" in rns_text:
                            minlease = rns_text.split("：")[1].replace(u"\xa0", u"")
                    
                    # 關閉請求，釋放內存
                    link_data.close()
                    await asyncio.sleep(int(random.uniform(1, 3)))

                    # minlease 有值即表示此link爬取成功，故跳出迴圈進行下一筆link的爬取
                    if minlease != "":
                        break
                except requests.exceptions.ConnectTimeout:
                    times += 1
                    print("Requests Timeout Error, link : ", link, "重試第", times, "次")
                    await asyncio.sleep(int(random.uniform(10, 15)))
                except requests.exceptions.SSLError:
                    times += 1
                    print("Requests SSL Error, link : ", link, "重試第", times, "次")
                    await asyncio.sleep(int(random.uniform(10, 15)))
                except requests.exceptions.ReadTimeout:
                    times += 1
                    print("Requests Read Timeout Error, link : ", link, "重試第", times, "次")
                    await asyncio.sleep(int(random.uniform(10, 15)))
                except requests.exceptions.ConnectionError as e:
                    times += 1
                    print(e, ", link : ", link, "重試第", times, "次")
                    await asyncio.sleep(int(random.uniform(50, 60)))
                except ssl.SSLError:
                    times += 1
                    print("SSL Error, link : ", link, "重試第", times, "次")
                    await asyncio.sleep(int(random.uniform(10, 15)))
                except urllib3.exceptions.ProtocolError:
                    times += 1
                    print("urlib3 Protocol Error, link : ", link, "重試第", times, "次")
                    await asyncio.sleep(int(random.uniform(10, 15)))


                if times == 5:
                    title = "Timeout"
                    landlord = ""
                    identity = ""
                    # link 抓的到
                    tel_num = ""
                    price = ""
                    housetype = ""
                    situation = ""
                    pings = ""
                    floor = ""
                    pattern = ""
                    genderrequire = ""
                    deposit = ""
                    minlease = ""
                    print(link, ", 重試", times, "次仍TIMEOUT")


            info = {
                'title' : title,
                'landlord' : landlord,
                'identity' : identity,
                'link' : link,
                'tel_num' : tel_num,
                'price' : price,
                'housetype' : housetype,
                'situation' : situation,
                'pings' : pings,
                'floor' : floor,
                'pattern' : pattern,
                'genderrequire' : genderrequire,
                'deposit' : deposit,
                'minlease' : minlease
                }
            
            info_list.append(info)
            counter += 1
            await asyncio.sleep(int(random.uniform(1, 3)))

        # print(info_list)
        #### 發現問題 : 會有跳出選擇縣市、通知，就會導致next page 失敗!!!!!!
        # 若還沒有到最後一頁，繼續點擊下一頁
        if i != totalpages-1:
            rentdb.insert_many(info_list)
            end = time.time()
            print("第", counter, "筆，第", i + 1, "頁，進度 : ", int((i+1)/totalpages*100), "%，執行時間 : ", int(end-start), "秒")
            await page.evaluate('_ => {window.scrollBy(0, window.innerHeight);}')
            await page.click('div.pageBar > a.pageNext > span')
            page.mouse
            await asyncio.sleep(int(random.uniform(3, 5)))
            # await page.waitFor(int(random.uniform(3, 7))*1000)
            await page.evaluate('_ => {window.scrollBy(0, window.innerHeight);}')
        # await asyncio.wait([
        #     page.click('div.pageBar > a.pageNext > span'),
        #     page.waitForNavigation(),
        # ])

    await browser.close()
    print("finish !")



if __name__ == '__main__':
    url = "https://rent.591.com.tw/"
    asyncio.get_event_loop().run_until_complete(main(url))