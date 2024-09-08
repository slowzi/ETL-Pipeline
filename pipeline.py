from helper.db_connector import postgres_engine_load
from helper.convert_currency import convert_to_idr
from helper.scraper_helper import soup2list
from helper.scraper_helper import extract_date
from helper.db_connector import postgres_engine_sales

from tabulate import tabulate
from tqdm import tqdm
import pandas as pd
import luigi
import requests
from time import sleep
from pangres import upsert
from bs4 import BeautifulSoup

class ExtractSalesDB(luigi.Task):
    def requires(self):
        pass

    def run(self):

        # init engine, fetch and read data from postgres db
        QUERY = "SELECT * FROM amazon_sales_data"
        engine = postgres_engine_sales()
        sales_db = pd.read_sql(sql=QUERY, con=engine)

        # export data extraction
        sales_db.to_csv(self.output().path, index=False)

    def output(self):
        return luigi.LocalTarget("data/raw/extract_sales_data.csv")
    

class ExtractProductDB(luigi.Task):
    def requires(self):
        pass

    def run(self):

        # init engineand read data from postgres db
        product_data = pd.read_csv("ElectronicsProductsPricingData.csv")

        # export data extraction
        product_data.to_csv(self.output().path, index=False)

    def output(self):
        return luigi.LocalTarget("data/raw/extract_product_data.csv")
    

class ExtractAmazonReviewDB(luigi.Task):
        
    def requires(self):
        pass

    def run(self):

        # Get and Convert the Timestamp object to a formatted string
        current_timestamp = pd.Timestamp.now()
        formatted_timestamp = current_timestamp.strftime('%Y-%m-%d %H:%M:%S')

        users = []
        ratings = []
        locations = []
        reviews = []
        titles = []
        date_experience = []

        from_page = 1
        to_page = 5
        company = 'www.amazon.com'

        # for i in range(from_page, to_page+1):
        for i in tqdm(range(from_page, to_page+1)):

            resp = requests.get(fr"https://www.trustpilot.com/review/{company}?page={i}")
            soup = BeautifulSoup(resp.content)

            # scrape review data from trustpilot
            soup2list(soup.find_all('span', {'class','typography_heading-xxs__QKBS8 typography_appearance-default__AAY17'}), users)
            soup2list(soup.find_all('div', {'class','typography_body-m__xgxZ_ typography_appearance-subtle__8_H2l styles_detailsIcon__Fo_ua'}), locations)
            soup2list(soup.find_all('div', {'class','styles_reviewHeader__iU9Px'}), ratings, attr='data-service-review-rating')
            soup2list(soup.find_all('h2', {'class','typography_heading-s__f7029 typography_appearance-default__AAY17'}), titles)
            # soup2list(soup.find_all('p', {'class','typography_body-l__KUYFJ typography_appearance-default__AAY17 typography_color-black__5LYEn'}), reviews)
            soup2list(soup.find_all('div', {'class','styles_reviewContent__0Q2Tg'}), reviews)
            soup2list(soup.find_all('p', {'class','typography_body-m__xgxZ_ typography_appearance-default__AAY17'}), date_experience)


            # To avoid throttling
            sleep(1)

        # get date and remove none value
        extract_dates = [extract_date(text) for text in date_experience]
        extract_dates = list(filter(lambda x: x is not None, extract_dates))

        amazonereview_data = pd.DataFrame({
        'user':users,
        'location':locations,
        'title':titles,
        'content':reviews,
        'rating': ratings,
        'experience_at':extract_dates,
        'scrape_at':formatted_timestamp
        })

        # export data extraction
        amazonereview_data.to_csv(self.output().path, index=False)

    def output(self):
        return luigi.LocalTarget("data/raw/extract_review_data.csv")


class ValidateData(luigi.Task):

    def requires(self):
        return [ExtractSalesDB(), ExtractProductDB(), ExtractAmazonReviewDB()]
    
    def run(self):

        for idx in range(0, 3):

            data = pd.read_csv(self.input()[idx].path)

            # start data quality pipeline
            print("===== Data Quality Pipeline Start =====")
            print("")

            # check data shape
            print("===== Check Data Shape =====")
            print("")
            print(f"Data Shape for this Data {data.shape}")

            # check data type
            get_cols = data.columns

            print("")
            print("===== Check Data Types =====")
            print("")

            column_type = []
            # iterate to each column
            for column in get_cols:
                column_type.append((column, data[column].dtypes))

            headers = ['review column', 'column types']
            print(tabulate(column_type, headers = headers, tablefmt='grid')) 

            # check missing values
            print("")
            print("===== Check Missing Values =====")
            print("")

            missing_pctg = []
            # iterate to each column
            for col in get_cols:

                # calculate missing values in percentage
                pctg = (data[col].isnull().sum() * 100) / len(data)
                missing_pctg.append((col, round(pctg, 2), data.shape[0]))

            headers = ['review column', 'missing values %', 'number of data']
            print(tabulate(missing_pctg, headers = headers, tablefmt='grid')) 

            print("===== Data Quality Pipeline End =====")
            print("")

            data.to_csv(self.output()[idx].path, index = False)

    def output(self):
        return [luigi.LocalTarget("data/validate/validate_sales_data.csv"),
                luigi.LocalTarget("data/validate/validate_product_data.csv"),
                luigi.LocalTarget("data/validate/validate_review_data.csv")]


class TransformSalesData(luigi.Task):

    def requires(self):
        return ValidateData()
    
    def run(self):
        # read data from previous task
        sales = pd.read_csv(self.input()[0].path)

        # fill value to actual and discount price
        sales['actual_price'] = sales['actual_price'].fillna(value = "0")
        sales['discount_price'] = sales['discount_price'].fillna(value = "0")

        # rate inr to idr
        rate = 190 

        # apply conversion
        sales['discount_price_idr'] = sales['discount_price'].apply(lambda x: convert_to_idr(x, rate))
        sales['actual_price_idr'] = sales['actual_price'].apply(lambda x: convert_to_idr(x, rate))

        # drop actual and discount column
        sales = sales.drop(columns=['discount_price', 'actual_price'])

        # ensure the new columns are integers
        CONV_COLS = ['discount_price_idr', 'actual_price_idr']
        sales[CONV_COLS] = sales[CONV_COLS].astype(float)

        # remove invalid value from rating
        INV_VALS = ["NaN", "free", "get", "₹70", "₹65", "₹99"]
        sales = sales[~sales['ratings'].isin(INV_VALS)]

        # remove "," and convert value from no_of_rating
        sales['no_of_ratings'] = sales['no_of_ratings'].astype(str)
        sales['no_of_ratings'] = sales['no_of_ratings'].str.replace(',', '')
        sales['no_of_ratings'] = sales['no_of_ratings'].replace('nan', pd.NA) 
        sales['no_of_ratings'] = sales['no_of_ratings'].fillna(sales['no_of_ratings'].mode().iloc[0])
        # sales['ratings'] = sales['ratings'].round().astype(int)

        # make ratings from 1 to 5
        # convert to float and fill null value to avg value
        sales['ratings'] = pd.to_numeric(sales['ratings'], errors='coerce')
        sales['ratings'] = sales['ratings'].clip(lower=1, upper=5)
        sales['ratings'] = sales['ratings'].fillna(sales['ratings'].mean())
        sales['ratings'] = sales['ratings'].round().astype(int)

        # drop unesecery column
        sales = sales.drop(columns=['Unnamed: 0'])

        # export to csv
        sales.to_csv(self.output().path, index = False)


    def output(self):
        return luigi.LocalTarget("data/transform/transform_sales_data.csv")


class TransformProductData(luigi.Task):

    def requires(self):
        return ValidateData()
    
    def run(self):
        # read data from previous task
        product = pd.read_csv(self.input()[1].path)

        SELECT_COLS = ['name', 'brand', 'primaryCategories', 'prices.currency',
               'prices.amountMax', 'prices.amountMin', 'prices.availability',
               'sourceURLs', 'dateAdded', 'dateUpdated']

        product = product[SELECT_COLS]

        RENAME_COLS = {
            'primaryCategories': 'categories', 
            'prices.currency': 'currency',
            'prices.amountMax': 'max_price', 
            'prices.amountMin': 'min_price', 
            'prices.availability': 'availability',
            'sourceURLs': 'urls', 
            'dateAdded': 'created_at', 
            'dateUpdated': 'updated_at'
        }

        product = product.rename(RENAME_COLS, axis=1)


        def usd_to_idr(price, rate):
            try:
                price_usd = float(price)
            except ValueError:
                return "Invalid Price"
            
            # Convert to IDR
            convert_price = price_usd * rate
            
            return convert_price

        # rate usd to idr
        rate = 15500 

        # apply conversion
        product['max_price'] = product['max_price'].apply(lambda x: usd_to_idr(x, rate))
        product['min_price'] = product['min_price'].apply(lambda x: usd_to_idr(x, rate))
                

        # convert to datetime object, handling 'Z' as UTC
        product['created_at'] = pd.to_datetime(product['created_at'], format='%Y-%m-%dT%H:%M:%SZ', utc=True)
        product['updated_at'] = pd.to_datetime(product['updated_at'], format='%Y-%m-%dT%H:%M:%SZ', utc=True)

        # create mapping availability
        AVAILABILITY_MAPPING = {
            'In Stock': ['In Stock', 'yes', 'Yes', 'TRUE'],
            'Out Of Stock': ['Out Of Stock', 'sold', 'No', 'FALSE'],
            'Undefined': ['undefined', 'Special Order', 'Retired', 'More on the Way', '32 available', '7 available']
        }

        reverse_mapping = {value: key for key, values in AVAILABILITY_MAPPING.items() for value in values}

        # map existing values to new categories
        product['availability'] = product['availability'].map(reverse_mapping).fillna('Undefined')

        # export to csv
        product.to_csv(self.output().path, index = False)


    def output(self):
        return luigi.LocalTarget("data/transform/transform_product_data.csv")


class TransformReviewData(luigi.Task):

    def requires(self):
        return ValidateData()
    
    def run(self):
        # read data from previous task
        review = pd.read_csv(self.input()[2].path)
        
        # convert to datetime
        review['experience_at'] = pd.to_datetime(review['experience_at'], format='%B %d, %Y')
        review['experience_at'] = review['experience_at'].dt.strftime('%Y-%m-%d')

        review['experience_at'] = pd.to_datetime(review['experience_at'], format='%Y-%m-%d')
        review['scrape_at'] = pd.to_datetime(review['scrape_at'])        

        # export to csv
        review.to_csv(self.output().path, index = False)


    def output(self):
        return luigi.LocalTarget("data/transform/transform_review_data.csv")


class LoadToDB(luigi.Task):
    
    def requires(self):
        return [TransformSalesData(),
                TransformProductData(),
                TransformReviewData()]
    

    def run(self):
        # init postgres engine load
        engine = postgres_engine_load()

        # read data from previous task
        load_sales_data = pd.read_csv(self.input()[0].path)
        load_product_data = pd.read_csv(self.input()[1].path)
        load_review_data = pd.read_csv(self.input()[2].path)

        load_sales_data.insert(0, 'id', range(0, 0 + len(load_sales_data)))
        load_product_data.insert(0, 'id', range(0, 0 + len(load_product_data)))
        load_review_data.insert(0, 'id', range(0, 0 + len(load_review_data)))

        load_sales_data = load_sales_data.set_index("id")
        load_product_data = load_product_data.set_index("id")
        load_review_data =load_review_data.set_index("id")

        # init table name for each task
        sales_db_name = "sales_db"
        product_db_name = "product_db"
        review_db_name = "review_db"

        upsert(con = engine, df = load_sales_data, table_name = sales_db_name, if_row_exists = "update")
        upsert(con = engine, df = load_product_data, table_name = product_db_name, if_row_exists = "update")
        upsert(con = engine, df = load_review_data, table_name = review_db_name, if_row_exists = "update")
        
        # save the process
        load_sales_data.to_csv(self.output()[0].path, index = False)
        load_product_data.to_csv(self.output()[1].path, index = False)
        load_review_data.to_csv(self.output()[2].path, index = False)


    def output(self):
        return [luigi.LocalTarget("data/load/load_sales_data.csv"),
                luigi.LocalTarget("data/load/load_product_data.csv"),
                luigi.LocalTarget("data/load/load_review_data.csv")]


if __name__ == "__main__":
    luigi.build([ExtractSalesDB(), ExtractProductDB(), ExtractAmazonReviewDB(),
                 ValidateData(), TransformSalesData(), TransformProductData(),
                 TransformReviewData(), LoadToDB()])