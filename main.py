from robocorp.tasks import task
from datetime import datetime
from uuid import uuid4
from dateutil.relativedelta import relativedelta
from RPA.Browser.Selenium import Selenium
from RPA.Robocorp.WorkItems import WorkItems
from RPA.Excel.Files import Files
from requests import Session
from concurrent.futures import ThreadPoolExecutor
import re
import logging
import os
import shutil


class NewsScraper:
    break_flag = False

    def __init__(self, url, search_phrase, category, number_of_months):
        self.browser = Selenium()
        self.url = url
        self.search_phrase = search_phrase 
        self.category = category
        self.path = self._create_results_directory()
        self.start_date = self._get_start_date(number_of_months)
        self.lib = self._create_activate_excel()
        self.session = Session()
        self.logger = self._get_logger()

    @staticmethod
    def _get_logger():
        logger = logging.getLogger(__name__)
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s - %(pathname)s:%(lineno)d'
        )
 
        return logger  

    def _create_results_directory(self):
        dir_name = self.search_phrase + " - " + datetime.now().strftime("%d %B %Y - %H-%M-%S")
        path = os.path.join('output', dir_name)
        os.makedirs(path, exist_ok=True)
        return path

    @staticmethod
    def _get_start_date(months):
        now = datetime.now()
        first_date_of_month = datetime(now.year, now.month, 1)
        if months in (0, 1):
            return first_date_of_month

        start_date = first_date_of_month - relativedelta(months=months - 1)
        return start_date

    def _create_activate_excel(self):
        lib = Files()
        lib.create_workbook(f"{self.path}/news.xlsx")
        lib.create_worksheet('news')
        lib.set_active_worksheet('news')
        return lib

    def _append_to_excel(self, page_news):
        self.lib.append_rows_to_worksheet(page_news, header=True)
        self.lib.save_workbook()

    def _zip_the_results(self):
        shutil.make_archive(self.path, 'zip', self.path)
        shutil.rmtree(self.path)

    def _open_site(self, url):
        self.browser.open_available_browser(url)
        self.logger.info("Site Opened")

    def _search(self, phrase):
        self.browser.click_button_when_visible('css:[data-element="search-button"]')
        self.browser.input_text('name:q', phrase)
        self.browser.click_button_when_visible('css:[data-element="search-submit-button"]')
        self.logger.info("Phrase searched")

    def _set_category(self, category):
        try:
            xpath = f"//div[@class='search-filter-input SearchFilterInput']//span[contains(text(), '{category}')]"
            page_count_xpath = f"//span[@class='search-results-module-count-desktop']"
            previous_text = self.browser.get_text(page_count_xpath)

            self.browser.click_element_when_visible(xpath)
            self.logger.info("Category Changed")

            self.browser.wait_until_element_does_not_contain(page_count_xpath, previous_text)  # wait

        except AssertionError:
            self.logger.warning(f"Category {category} Not Found!")
            pass

    def _change_order(self, order):
        self.browser.wait_until_page_contains_element('name:s')
        self.browser.select_from_list_by_label('name:s', order)

        xpath = "//option[contains(text(), 'Newest') and @selected]"
        self.browser.wait_until_page_contains_element(xpath)  # wait

        self.logger.info("Order Changed")

    def _save_img(self, img_src):
        response = self.session.get(img_src, stream=True)
        img_name = uuid4()
        with open(f'{self.path}/{img_name}.png', "wb") as img:
            shutil.copyfileobj(response.raw, img)
        return img_name

    def _extract_data(self, i,post):
        title,date,description,picture=post
        timestamp = int(date.get_attribute("data-timestamp")) / 1000
        date = datetime.fromtimestamp(timestamp)
        if date < self.start_date:
            self.break_flag = True
            return i, None

        result = {"date": date.strftime("%m/%d/%Y")}

        text = title.text
        result['title'], result['count'] = text, text.lower().count(self.search_phrase.lower())

        desc = description.text
        result['description'] = desc
        result['count'] += desc.lower().count(self.search_phrase.lower())
        
        if picture:
            img_name = self._save_img(picture.get_attribute("src"))
            result['pic'] = f'{img_name}.png'
        else:
            result['pic']="No Image!"

        pattern = re.compile(r"\$\d+(\.\d{1,2})?|\$\d{1,3}(,\d{3})*(\.\d{1,2})?|\d+\s(dollars|USD)")
        result['contains_money'] = re.search(pattern, result['title'] + result['description']) is not None
        return i, result

    def _get_page_elements(self): 
        posts = self.browser.find_elements('css:.promo-wrapper')
        
 
        def fetch_post_elements(css_selector):
            title=self.browser.find_element('css:.promo-title .link')
            date=self.browser.find_element('css:.promo-timestamp' )
            description=self.browser.find_element('css:.promo-description')
            try: 
                picture=self.browser.find_element('css:.promo-media .image')
            except:
                self.logger.warning("Picture Not Found!")
                picture=None
            
            return title,date,description,picture
        
        with ThreadPoolExecutor() as executor:
            posts_elements=executor.map(fetch_post_elements,posts)
                
        return list(posts_elements)

    @staticmethod
    def _sort_elements_data(data):
        data = sorted([d for d in data if d[1]], key=lambda l: l[0])
        data = [d[1] for d in data]
        return list(data)

    def _get_page_news(self):
        self.logger.info("Fetching Data...") 
        posts_elements = self._get_page_elements()
        number_of_news = len(posts_elements)

        with ThreadPoolExecutor() as executor:
            data = executor.map(self._extract_data, range(number_of_news), posts_elements)
         
        self.logger.info("Data Fetched!")
        return self._sort_elements_data(data)

    def _get_number_of_pages(self):
        self.browser.wait_until_page_contains_element("css:.search-results-module-page-counts")
        number_of_pages = self.browser.find_element("css:.search-results-module-page-counts")
        number_of_pages = int(number_of_pages.text.split("of")[1].replace(" ", "").replace(",", ""))
        return number_of_pages

    def _go_to_next_page(self):
        xpath = f"//div[@class='search-results-module-next-page']//a"
        next_page_available = self.browser.is_element_visible(xpath)
        if not next_page_available:
            self.logger.info("End of Pages!")
            return False
        self.browser.click_element_when_visible("css:.search-results-module-next-page")
        self.logger.info("going to next Page!")
        return True

    def _get_pages_news(self):
        number_of_pages = self._get_number_of_pages()

        for page in range(number_of_pages):

            page_news = self._get_page_news()
            
            print(page_news) 

            yield page_news

            if self.break_flag: 
                break

            next_page_exists = self._go_to_next_page()
            if not next_page_exists:
                break

    def _excel_output(self):
        self.logger.info("Getting News Started...")
        for news_page in self._get_pages_news():
            self._append_to_excel(news_page)

        self.logger.info("Excel Created")

    def run(self):
        try:
            self._open_site(self.url)
            self._search(self.search_phrase)
            # self._change_order("Newest") 
            # self._set_category(self.category)
            self._excel_output() 
        except Exception as e:
            self.logger.error(e)
            
        finally: 
            self._zip_the_results()
            self.browser.close_browser()
            


@task
def Scraper(): 
    # library = WorkItems()
    # library.get_input_work_item()
    # variables = library.get_work_item_variables() 
 
    s = NewsScraper("https://www.latimes.com/", "price", "dsd", 5000)
    s.run()
