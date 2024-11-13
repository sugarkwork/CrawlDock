import os
import re
import json
import time
import io
import logging
from typing import Optional, List, Dict, Tuple
import shutil
from concurrent.futures import ThreadPoolExecutor, as_completed
import hashlib
import random

import requests
from urllib.parse import urljoin
from PIL import Image
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.common.by import By
from dataclasses import dataclass, field
import trafilatura
import pypdf


# ロギング設定
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("docker_selenium")

@dataclass
class DockerConfig:
    image: str = "selenium/standalone-chrome"
    network_prefix: str = "127.0.0"
    selenium_port: int = 4444
    vnc_port: int = 7900
    window_width: int = 1920
    window_height: int = 1080
    wait_timeout: int = 20

@dataclass
class DockerContainer:
    url: str
    name: str
    download_dir: str


class LinkInfo:
    def __init__(self, context: str, link_text: str, url: str, scraper: 'WebPageScraper', cookie_name: str = None):
        self.context = context
        self.link_text = link_text
        self.url = url
        self.scraper = scraper
        self.cookie_name = cookie_name

    def __str__(self):
        return f"""
Context: {self.context}
Link Text: {self.link_text}
URL: {self.url}
""".strip()
    
    def __repr__(self):
        return str(self)
    
    def click(self):
        return self.scraper.click_link_and_get_html(self)
    
    def get(self):
        return self.scraper.get(self.url, self.cookie_name)


class DockerSeleniumManager:
    def __init__(self, config: DockerConfig = DockerConfig()):
        self.config = config
        self.docker_info = []
    
    def get_selenium_docker(self) -> Optional[DockerContainer]:
        docker = self.start_selenium_docker()
        if not docker:
            return None
        return docker
    
    def update_docker_info(self):
        dockers = os.popen(f"docker ps --filter name=selenium --format json").read().strip().split("\n")
        self.docker_info = [json.loads(docker) for docker in dockers if docker]
        return self.docker_info

    def get_free_docker_id(self) -> Optional[int]:
        try:
            exist_ids = []
            for docker in self.docker_info:
                if docker["Image"] == self.config.image and docker["State"] == "running":
                    name = str(docker["Names"]).strip()
                    id = name.replace("selenium-chrome", "")
                    exist_ids.append(int(id) if id else 0)

            free_ids = []
            for i in range(1, 250):
                if i not in exist_ids:
                    free_ids.append(i)
            
            random.shuffle(free_ids)

            if free_ids:
                return free_ids[0]

            return None
        except Exception as e:
            logger.error(f"Error getting free docker ID: {e}")
            return None

    def start_selenium_docker(self) -> Optional[DockerContainer]:
        try:
            docker_id = self.get_free_docker_id()
            if docker_id is None:
                raise Exception("No available Docker ID")

            docker_name = f"selenium-chrome{docker_id}"
            docker_url = f"{self.config.network_prefix}.{docker_id}"
            download_dir = os.path.abspath(os.path.join("downloads", docker_name)) # ダウンロードディレクトリ

            if os.path.exists(download_dir):
                shutil.rmtree(download_dir, ignore_errors=True)

            os.makedirs(download_dir)

            cmd = (
                f"docker run " # コンテナの起動
                f"--rm " # 終了時にコンテナを削除
                f" -d " # バックグラウンドで実行
                f"--name {docker_name} " # コンテナ名
                f"-v {download_dir}:/downloads " # ダウンロードディレクトリのマウント
                f"-p {docker_url}:{self.config.selenium_port}:{self.config.selenium_port} " # Seleniumポートのマッピング
                f"-p {docker_url}:{self.config.vnc_port}:{self.config.vnc_port} " # VNCポートのマッピング
                f"--shm-size=\"8g\" " # 共有メモリサイズの指定
                f"{self.config.image}" # Dockerイメージ
            )

            if os.system(cmd) != 0:
                raise Exception("Failed to start Docker container")

            if not self.wait_for_selenium_server(docker_url):
                raise Exception("Selenium server failed to start")

            return DockerContainer(url=docker_url, name=docker_name, download_dir=download_dir)
        except Exception as e:
            logger.error(f"Error starting selenium docker: {e}")
            return None

    def wait_for_selenium_server(self, url: str, timeout: int = 30) -> bool:
        logger.info(f"Waiting for Selenium server at {url}")
        start_time = time.time()
        while time.time() - start_time < timeout:
            try:
                response = requests.get(f"http://{url}:{self.config.selenium_port}/wd/hub/status")
                if response.status_code == 200:
                    logger.info("Selenium server is ready")
                    return True
            except:
                time.sleep(1)
        return False

    def stop_docker_container(self, container: DockerContainer):
        try:
            os.system(f"docker stop {container.name}")
        except Exception as e:
            logger.error(f"Error stopping docker container: {e}")

class WebPageScraper:
    def __init__(self, docker_manager: DockerSeleniumManager = None):
        self.docker_manager = docker_manager
        if not self.docker_manager:
            self.docker_manager = DockerSeleniumManager()
        self.driver = None
        self.docker_container = None
    
    def start_driver(self):        
        self.docker_container = self.docker_manager.get_selenium_docker()
        if not self.docker_container:
            raise Exception("Failed to start Docker container")

        selenium_url = f"http://{self.docker_container.url}:{self.docker_manager.config.selenium_port}/wd/hub"
        options = webdriver.ChromeOptions()
        options.add_experimental_option('prefs', {
            "download.default_directory": "/downloads", #Change default directory for downloads
            "download.prompt_for_download": False, #To auto download the file
            "download.directory_upgrade": True, #It will not show download bar for PDFs
            "plugins.always_open_pdf_externally": True #It will not show PDF directly in chrome
        })

        self.driver = webdriver.Remote(
            command_executor=selenium_url,
            options=options
        )

        self.driver.set_window_size(self.docker_manager.config.window_width, 
                                self.docker_manager.config.window_height)

    def get(self, url: str, cookie_name: str = None) -> Tuple[Optional[str], Optional[List[LinkInfo]], Optional[Image.Image]]:
        if self.driver is None:
            self.start_driver()

        try:
            logger.info(f"Starting to fetch page: {url}")
            
            self.driver.get(url)

            if cookie_name:
                cookies = CookieManager.load_cookies(cookie_name)

                for cookie in cookies:
                    self.driver.add_cookie(cookie)

                self.driver.get(url)

            return self._return_page_html(url, cookie_name)
        except Exception as e:
            logger.error(f"Error in get: {e}")
            return None, None, [], None

    def _return_page_html(self, url: str, cookie_name: str = None) -> Tuple[Optional[str], Optional[List[LinkInfo]], Optional[Image.Image]]:
        try:
            # ページの読み込み待機
            WebDriverWait(self.driver, self.docker_manager.config.wait_timeout).until(
                EC.presence_of_element_located((By.TAG_NAME, "body"))
            )

            WebDriverWait(self.driver, self.docker_manager.config.wait_timeout).until(
                lambda driver: driver.execute_script("return document.readyState") == "complete"
            )

            # ページソースの取得
            page_html = self.driver.page_source

            # png bytes to Image
            screenshot = Image.open(io.BytesIO(self.driver.get_screenshot_as_png()))

            links = self.extract_links(url, page_html)

            if cookie_name:
                CookieManager.save_cookies(self.driver.get_cookies(), cookie_name)
            
            page_text = None
            try:
                page_text = trafilatura.extract(page_html, url=url, output_format="markdown", include_links=True)
            except Exception as e:
                logger.error(f"Error extracting text: {e}")

            if not page_text:
                logger.warning("No page HTML found")

                download_dir = self.docker_container.download_dir
                download_files = os.listdir(download_dir)
                if not download_files:
                    logger.info("No files downloaded")
                else:
                    for file in download_files:
                        file = os.path.join(download_dir, file)
                        if file.endswith(".pdf"):
                            logger.info(f"Found PDF file: {file}")
                            reader = pypdf.PdfReader(file)
                            result = ""
                            for page in reader.pages:
                                text = page.extract_text()
                                result += text
                            page_text = result
                            page_html = result
                            links = []
                            screenshot = None
                            break

            return page_html, page_text, links, screenshot

        except Exception as e:
            logger.error(f"Error in get: {e}")
            return None, None, [], None

    def stop_driver(self):
        if self.driver:
            self.driver.quit()
        if self.docker_container:
            self.docker_manager.stop_docker_container(self.docker_container)

        shutil.rmtree(self.docker_container.download_dir, ignore_errors=True)


    def extract_links(self, base_url: str, html: str = None, context_chars: int = 200) -> List[LinkInfo]:
        try:
            if not html:
                return []

            soup = BeautifulSoup(html, 'html.parser')
            links = []

            for a_tag in soup.find_all('a', href=True):
                # リンクのURLを取得
                url = a_tag.get('href', '')
                
                # 相対URLを絶対URLに変換
                if url.startswith('/'):
                    url = urljoin(base_url, url)
                
                # リンクのテキストを取得
                link_text = a_tag.get_text(strip=True)
                
                # リンクの前後の文脈を取得
                context = self._get_link_context(a_tag, context_chars)
                
                # リンク情報を追加
                if url and link_text:
                    if url.startswith("#"):
                        url = urljoin(base_url, url)
                    links.append(LinkInfo(
                        context=context,
                        link_text=link_text,
                        url=url,
                        scraper=self
                    ))

            return links

        except Exception as e:
            logger.error(f"Error extracting links: {e}")
            return []

    def _get_link_context(self, a_tag, context_chars: int) -> str:
        try:
            # リンクを含む段落や要素を見つける
            parent = a_tag.find_parent(['p', 'div', 'section', 'article'])
            if not parent:
                return ""

            # 親要素のテキストを取得
            full_text = parent.get_text(strip=True)
            full_text2 = re.sub(r'\s+', ' ', parent.get_text()).strip()
            
            # リンクテキストの位置を特定
            link_text = a_tag.get_text(strip=True)
            link_text2 = re.sub(r'\s+', ' ', a_tag.get_text()).strip()
            link_pos = full_text.find(link_text)
            
            if link_pos == -1:
                return ""

            # 前後の文脈を抽出
            start = max(0, link_pos - context_chars)
            end = min(len(full_text2), link_pos + len(link_text2) + context_chars)
            
            # 文脈を整形
            #context = full_text[start:end]
            context = full_text2[start:end]
            if start > 0:
                context = f"...{context}"
            if end < len(full_text):
                context = f"{context}..."

            return context

        except Exception as e:
            logger.error(f"Error getting link context: {e}")
            return ""

    def click_link_and_get_html(self, link_info: LinkInfo) -> Tuple[Optional[str], Optional[Image.Image]]:
        try:
            logger.info(f"Clicking link: {link_info.url}")
            logger.debug(link_info)

            link_element = None

            if not link_element:
                try:
                    link_element = self.driver.find_element(By.LINK_TEXT, link_info.link_text)
                except:
                    pass
            
            if not link_element:
                try:
                    # 2. 部分一致検索
                    link_element = self.driver.find_element(By.PARTIAL_LINK_TEXT, link_info.link_text)
                except:
                    pass
            
            if not link_element:
                try:
                    # 3. XPathを使用
                    link_element = self.driver.find_element(By.XPATH, f"//a[contains(text(), '{link_info.link_text}')]")
                except:
                    pass

            click_success = False
            if link_element:
                try:
                    link_element.click()
                    click_success = True
                except:
                    pass

                if not click_success:
                    try:
                        self.driver.execute_script("arguments[0].click();", link_element)
                        click_success = True
                    except:
                        pass
                
                if not click_success:
                    try:
                        ActionChains(self.driver).move_to_element(link_element).click().perform()
                        click_success = True
                    except:
                        pass
                
            if click_success:
                return self._return_page_html(link_info.url, link_info.cookie_name)
            else:
                logger.error("Could not click link")
                return None, None, [], None
        
        except Exception as e:
            logger.error(f"Error clicking link: {e}")
            return None, None, [], None

class CookieManager:
    @staticmethod
    def load_cookies(filename: str) -> List[Dict]:
        try:
            if os.path.exists(filename):
                with open(filename, "r") as f:
                    return json.load(f)
        except Exception as e:
            logger.error(f"Error loading cookies: {e}")
        return []

    @staticmethod
    def save_cookies(cookies: List[Dict], filename: str) -> None:
        try:
            with open(filename, "w") as f:
                json.dump(cookies, f)
        except Exception as e:
            logger.error(f"Error saving cookies: {e}")

    @staticmethod
    def add_cookies_to_driver(driver, cookies: List[Dict]):
        """
        ドライバーにクッキーを追加する
        
        Args:
            driver: Seleniumウェブドライバー
            cookies: クッキーのリスト
        """
        for cookie in cookies:
            try:
                driver.add_cookie(cookie)
            except Exception as e:
                logger.warning(f"Could not add cookie {cookie.get('name', 'Unknown')}: {e}")


def main(base_url = "https://lindajosiah.medium.com/python-selenium-docker-downloading-and-saving-files-ebb9ab8b2039"):
    scraper = WebPageScraper()

    try:
        result1, result_text1, links1, image1 = scraper.get(base_url)

        if result_text1:
            print(result_text1[:min(500, len(result_text1))])
        else:
            print("No result")

        print("------------------------------------")

        for link in links1:
            print(link)
            break

        if image1:
            image1.save(hashlib.sha1(base_url.encode()).hexdigest() + "_1.png")

        if links1:

            print("------------------------------------")

            link = links1[0]
            result, result_text, links, image = link.click()

            if result_text:
                print(result_text[:min(500, len(result_text))])
            else:
                print("No result")
            
            print("------------------------------------")
            for link in links:
                print(link)
                break

            if image:
                image.save(hashlib.sha1(base_url.encode()).hexdigest() + "_2.png")

        return result_text1
    finally:
        scraper.stop_driver()

def main2():
    # threadpool multi thread access test
    urls = [
        "https://zenn.dev/quiver/articles/21c2978cf869db",
        "https://blog.grasswake.me/tech-posts/1615906800",
    ]

    results = {}

    with ThreadPoolExecutor(max_workers=5) as executor:
        future_to_url = {
            executor.submit(main, url): url 
            for url in urls
        }
        for future in as_completed(future_to_url):
            url = future_to_url[future]
            try:
                text = future.result()
                results[url] = {
                    'text': text
                }
            except Exception as e:
                results[url] = {'error': str(e)}

    for url, result in results.items():
        print(f"URL: {url}")
        if 'error' in result:
            print(f"Error: {result.get('error')}")
        else:
            print(f"text: {result['text']}")

# メイン実行部
if __name__ == "__main__":
    main2()
