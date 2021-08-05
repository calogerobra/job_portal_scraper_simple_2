# Import general libraries
import datetime
import pandas as pd
from bs4 import BeautifulSoup as soup
import time
import re
import os

# SQl packages
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
import sqlalchemy

import requests
requests.packages.urllib3.disable_warnings()
import random

def adjust_listings_pages(page, pagelist):
    """ Adjusts listings to restart properly after crash
    Args:
        Current page
        Amount of pages
        Parameter if query is set on repeat
    Returns:
        Jumps to page where last stopped
    """
    return pagelist[pagelist.index(page):len(pagelist)]


def join_url(*args):
    """Constructs an URL from multiple URL fragments.
    Args:
        *args: List of URL fragments to join.
    Returns:
        str: Joined URL.
    """
    return ''.join(args)

def transform_to_pagecount(pagecount):
    """ Transforms number of pages to pagecount.
    Args:
        pagecount
    Returns:
        number of listings to be entered in teh URL
    """
    if int(pagecount) == 1:
        nlisting = 0
    elif int(pagecount) == 2:
        nlisting = 20
    elif int(pagecount) > 2:
        nlisting = int(pagecount) * 10 + 20
    return nlisting

def construct_listing_url(base_url, key_word, job_category, region, pagecount):
    """Constructs URLs for listing parameters.
    Args:
        key_word: Key word for job search
        job_category: category of job (numeric input from webpage)
        region: region for job (numeric entry from webpage, all is -1)
    Returns:
        str: An URL for the requested listing parameters.
    """
    # modify this here with exceptions
    constructed_url = join_url(base_url, 'f=', str(transform_to_pagecount(pagecount)),
    '&action=search&auth_sess=nujrnbrdjju8jvfb75irf6vt57&ref=1cb05c4d18430f917eb3748c1&jids[]=',
    str(job_category), "&lids[]=", str(region), "&kwd=", str(key_word))
    return constructed_url + "&cmdSearch=K%CBRKO&o_type=1"

def request_page(url_string, verification):
    """HTTP GET Request to URL.
    Args:
        url_string (str): The URL to request.
    """
    #time.sleep(random.randint(1,3))
    uclient = requests.get(url_string, timeout = 60, verify = verification)
    page_html = uclient.text
    return page_html

def set_max_page(page_soup):
    """Sets the maximum page number of the current search.
    Args:
        page_soup element
    Returns:
        Amount of pages of the current query
    """
    pagecount_container = page_soup.findAll("table", {"bgcolor":"#FFFFFF"})[0].tr.td.b.text.replace("(", "").replace(")", "")
    pagecount_container = [int(s) for s in pagecount_container.split() if s.isdigit()]
    return round((int(pagecount_container[2])/20))

def create_elements(container):
    """Extracts the relevant information form the html container, i.e. object_id,
    Args:
        A container element + region, city, districts, url_string.
    Returns:
        A dictionary containing the information for one listing.
    """
    object_link = str(container.a["href"])
    object_id_container = object_link.split("/")
    object_id = int(object_id_container[len(object_id_container)-2])
    object_title = str(container.a.text)
    # Create a dictionary as output
    return dict([("object_link", object_link),
                 ("object_id", object_id),
                 ("object_title", object_title)])

def reveal_link(input_dict):
    """ Reveals the object link of the listing currently in loop
    Args:
        input dictionary from elements creator
    Returns:
        object id (also referred to as listing id or object id)
    """
    return input_dict['object_link']

def reveal_id(input_dict):
    """ Reveals the object id of the listing currently in loop
    Args:
        input dictionary from elements creator
    Returns:
        object id (also referred to as listing id or object id)
    """
    return input_dict['object_id']


def make_listings_soup(object_link, verification):
    """ Create soup of listing-specific webpage
    Args:
        object_id
    Returns:
        soup element containing listings-specific information
    """
    listing_url = object_link
    return soup(request_page(listing_url, verification), 'html.parser')

def add_contents(listing_soup):
    """ Add contents from the listing to the current input dictionary
    Args:
        A listing bs4 elemnt, listing_soup
    Returns:
        Dictionary with listings-specific content
    """
    # 1. Set max length of container elements
    maxlength = len(listing_soup.findAll("table", {"id": "idviewjob"})[0].findAll("tr"))

    # 2. Extract ambiguous elements
    # Extract company name
    found = False
    for i in range(1,maxlength):
        if "Kompania:" in listing_soup.findAll("table", {"id": "idviewjob"})[0].findAll("tr")[i].text:
            company_name = listing_soup.findAll("table", {"id": "idviewjob"})[0].findAll("tr")[i].text.replace("Kompania:", "").replace("\r","").replace("\n","")
            found = True
        elif found == False and i < maxlength-1:
            found = False
            continue
        elif found == False and i == maxlength-1:
            found = False
            company_name = ""

    # Extract job category
    found = False
    for i in range(1,maxlength):
        if "Kategoria e punës:" in listing_soup.findAll("table", {"id": "idviewjob"})[0].findAll("tr")[i].text:
            job_category = listing_soup.findAll("table", {"id": "idviewjob"})[0].findAll("tr")[i].text.replace("Kategoria e punës:", ""). \
            replace("\r","").replace("\n","").replace("\xa0", "").replace("[Të gjitha punët në kategorinë Menaxhment]", "")
            found = True
        elif found == False and i < maxlength-1:
            found = False
            continue
        elif found == False and i == maxlength-1:
            found = False
            job_category = ""
        if len(job_category) > 0:
            job_category = job_category.split("[")[0]

    # Extract job description
    job_description = listing_soup.findAll("td", {"class":"td4textarea"})[0]. \
    text.replace("xa0","").replace("\x95\xa0\xa0\xa0\xa0\xa0\xa0\xa0\xa0",""). \
    replace("\r", "")

    # Extract skills
    found = False
    for i in range(1,maxlength):
        if "Shkathtësitë:" in listing_soup.findAll("table", {"id": "idviewjob"})[0].findAll("tr")[i].text:
            skills = listing_soup.findAll("table", {"id": "idviewjob"})[0].findAll("tr")[i].text.replace("Shkathtësitë:", "").replace("\r","").replace("\n\n- ","")
            found = True
        elif found == False and i < maxlength-1:
            found = False
            continue
        elif found == False and i == maxlength-1:
            found = False
            skills = ""
    if len(skills) > 0:
        skills_container = skills.split("\n")
        skills = skills = "|".join(skills_container)
        skills = skills.replace("||","|").replace("||","|")
        if len(skills) == 1 and skills == "|":
            skills = skills.replace("|","")

    # Extract language requirement
    language_requirements_container = listing_soup.findAll("td", {"class":"ulli"})[0].findAll("li")
    language_requirements = '|'.join([i.text for i in language_requirements_container])

    # Extract type of contract
    found = False
    for i in range(1,maxlength):
        if "Lloji i punësimit:" in listing_soup.findAll("table", {"id": "idviewjob"})[0].findAll("tr")[i].text:
            type_of_contract = listing_soup.findAll("table", {"id": "idviewjob"})[0].findAll("tr")[i].text.replace("Lloji i punësimit:", "").replace("\n","")
            found = True
        elif found == False and i < maxlength-1:
            found = False
            continue
        elif found == False and i == maxlength-1:
            found = False
            type_of_contract = ""

    # Extract wage
    found = False
    for i in range(1,maxlength):
        if "Paga:" in listing_soup.findAll("table", {"id": "idviewjob"})[0].findAll("tr")[i].text:
            wage = listing_soup.findAll("table", {"id": "idviewjob"})[0].findAll("tr")[i].text.replace("\nPaga:\n", "").replace("\n","")
            found = True
        elif found == False and i < maxlength-1:
            found = False
            continue
        elif found == False and i == maxlength-1:
            found = False
            wage = ""

    # Extract min. degree
    found = False
    for i in range(1,maxlength):
        if "Diploma:" in listing_soup.findAll("table", {"id": "idviewjob"})[0].findAll("tr")[i].text:
            min_degree = listing_soup.findAll("table", {"id": "idviewjob"})[0].findAll("tr")[i].text.replace("Diploma:", "").replace("\n","").replace("\n","")
            found = True
        elif found == False and i < maxlength-1:
            found = False
            continue
        elif found == False and i == maxlength-1:
            found = False
            min_degree = ""

    # Extract years of experience
    found = False
    for i in range(1,maxlength):
        if "Përvoja (vite):" in listing_soup.findAll("table", {"id": "idviewjob"})[0].findAll("tr")[i].text:
            years_of_experience = listing_soup.findAll("table", {"id": "idviewjob"})[0].findAll("tr")[i].text.replace("Përvoja (vite):", "").replace("\n","").replace("\n","")
            found = True
        elif found == False and i < maxlength-1:
            found = False
            continue
        elif found == False and i == maxlength-1:
            found = False
            years_of_experience = ""

    #Extract job location
    found = False
    for i in range(1,maxlength):
        if "Lokacioni i punës:" in listing_soup.findAll("table", {"id": "idviewjob"})[0].findAll("tr")[i].text:
            job_location = listing_soup.findAll("table", {"id": "idviewjob"})[0].findAll("tr")[i].text.replace("Lokacioni i punës:", "").replace("\n","").replace("\n","")
            found = True
        elif found == False and i < maxlength-1:
            found = False
            continue
        elif found == False and i == maxlength-1:
            found = False
            job_location = ""

    # Extract full address
    found = False
    for i in range(1,maxlength):
        if "Adresa:" in listing_soup.findAll("table", {"id": "idviewjob"})[0].findAll("tr")[i].text:
            full_address = listing_soup.findAll("table", {"id": "idviewjob"})[0].findAll("tr")[i].text.replace("Adresa:", "").replace("\n","").replace("\n","")
            found = True
        elif found == False and i < maxlength-1:
            found = False
            continue
        elif found == False and i == maxlength-1:
            found = False
            full_address = ""

    # Extract zip code
    found = False
    for i in range(1,maxlength):
        if "Kodi postar:" in listing_soup.findAll("table", {"id": "idviewjob"})[0].findAll("tr")[i].text:
            zip_code = listing_soup.findAll("table", {"id": "idviewjob"})[0].findAll("tr")[i].text.replace("Kodi postar:", "").replace("\n","").replace("\n","")
            found = True
        elif found == False and i < maxlength-1:
            found = False
            continue
        elif found == False and i == maxlength-1:
            found = False
            zip_code = ""

    # Extract posting date
    found = False
    for i in range(1,maxlength):
        if "Data e shpalljes:" in listing_soup.findAll("table", {"id": "idviewjob"})[0].findAll("tr")[i].text:
            posting_date = listing_soup.findAll("table", {"id": "idviewjob"})[0].findAll("tr")[i].text.replace("Data e shpalljes:", ""). \
            replace("\n","").replace("\n","").replace(" / See total 407 times","").replace(" ","")
            found = True
        elif found == False and i < maxlength-1:
            found = False
            continue
        elif found == False and i == maxlength-1:
            found = False
            posting_date = ""
    if len(posting_date) > 0:
        posting_date_container = posting_date.split("/")
        posting_date = posting_date_container[2] + "-" + posting_date_container[1] + "-" + posting_date_container[0]

    # Extract phone number
    found = False
    for i in range(1,maxlength):
        if "Kontakt tel.:" in listing_soup.findAll("table", {"id": "idviewjob"})[0].findAll("tr")[i].text:
            phone_number = listing_soup.findAll("table", {"id": "idviewjob"})[0].findAll("tr")[i].text.replace("Kontakt tel.:", ""). \
            replace("\n","").replace("\n","").replace(" ","")
            found = True
        elif found == False and i < maxlength-1:
            found = False
            continue
        elif found == False and i == maxlength-1:
            found = False
            phone_number = ""

    # 3. Export dictionary
    return  dict([('company_name', company_name),
                  ('phone_number', phone_number),
                  ('posting_date', posting_date),
                  ('job_category', job_category),
                  ('job_location', job_location),
                  ('full_address', full_address),
                  ('zip_code', zip_code),
                  ('years_of_experience', years_of_experience),
                  ('min_degree', min_degree),
                  ('wage', wage),
                  ('type_of_contract', type_of_contract),
                  ('skills', skills),
                  ('job_description', job_description),
                  ('language_requirements', language_requirements)])

def save_html_to_text(listing_soup, listing_textfile_path, now_str, object_id):
    """ Saves each listing as backup in a seperate text file.
    Args:
        beautioful soup elemnt for listing
        output path
        now string for timestamp
        object_id for link to SQL table
    """
    listing_soup_b = listing_soup.prettify("utf-8")
    time_folder = listing_textfile_path + now_str
    tfile_name = time_folder + "//" + now_str + "_listing_" + str(object_id) + ".txt"
    with open(tfile_name, "wb") as text_file:
        return text_file.write(listing_soup_b)


def scrape_ofertapune(maxpage, key_word, job_category, region, max_repeats, base_url, verification, now_str, listing_textfile_path):
    """Scraper forPortalpune job portal based on specified parameters.
    In the following we would like to extract all the containers containing
    the information on one listing. For this purpose we try to parse through
    the html text and search for all elements of interest.
    Args:
        input parameters dor search (key_word, job_category, region)
        maxpage for initial start
        path for pdf and jpeg files
        base_url for start
        verification input for ssl certificate
        current time stamp (now_str)
    Returns:
        Appended pandas dataframe with crawled content.
    """
    # Define dictionary for output
    input_dict = {}
    frames = []
    on_repeat = False
    first_run = True
    counter = 0
    skipper = 0
    pagelist = list(range(1,maxpage))
    # Loop over pages
    while on_repeat or first_run:
        counter += 1
        if counter >= max_repeats:
            break
        print("Running iteration", counter, "of parser...")
        try:
            for page in pagelist:
                # 1. Correct pagecount
                pagelist = adjust_listings_pages(page, pagelist)
                # 2. Set URL string
                url_string = construct_listing_url(base_url, key_word, job_category, region, str(page))
                # 3. Now let's parse through the page that we previously stored and do the scraping
                time.sleep(random.randint(0,1))
                page_soup = soup(request_page(url_string, verification), 'html.parser') # datatype from beautiful soup
                # 4. Grab pagecount to avoid repititions
                maxpage = set_max_page(page_soup)
                # 5. Grab all listings on the page
                print("Reading page", page, "of", maxpage, "...")
                containers_1 = page_soup.findAll("tr" , {"class":"phpjob_listfeatured"} )
                containers_2 = page_soup.findAll("tr" , {"class":"phpjob_listbgcolor1"} )
                containers_3 = page_soup.findAll("tr" , {"class":"phpjob_listbgcolor2"} )
                containers = containers_1 + containers_2 + containers_3
                len(containers) # verifies that we have e.g. 20 postings
                # 6. Iterate over containers
                print("Reading out", len(containers), "containers..." )
                for container in containers:
                    now = datetime.datetime.now()
                    try:
                        # 6.1. Create a dictionary with main content of front page
                        input_dict = create_elements(container)
                        # 6.2 Create soup for individual listing
                        time.sleep(random.randint(0,1))
                        listing_soup = make_listings_soup(reveal_link(input_dict), verification)
                        # 6.4. Add further contents
                        input_dict.update(add_contents(listing_soup))
                        # 6.5 Save listing
                        save_html_to_text(listing_soup, listing_textfile_path,  now_str, reveal_id(input_dict))
                        # 6.6. Create a dataframe
                        df = pd.DataFrame(data = input_dict, index =[now])
                        df.index.names = ['scraping_time']
                        frames.append(df)
                    except (IndexError, ValueError):
                        print("Ecountered problem, skipping container...")
                        skipper += 1
                        print("No. of skips so far: ", skipper)
                        continue
                # 7. Break out of loop if maxpage is hit
                if page == maxpage:
                    break # Break loop to avoid repititions once maxpage is hit
            first_run = False
            on_repeat = False
            return pd.concat(frames).drop_duplicates(subset = 'object_link') # Note: This is key to reduce the duplicates in the dataset
        except requests.exceptions.ConnectionError:
            print("Connection was interrupted, waiting a few moments before continuing...")
            time.sleep(random.randint(2,5) + counter)
            on_repeat = True
            continue
        except sqlalchemy.exc.DatabaseError or requests.exceptions.SSLError:
            break

def main():
    """ Note: Set parameters in this function
    """
    # Set now string
    now_str = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")

    # Set key word
    key_word = ''

    # Set region
    region = '529'

    # Set job category
    job_category = '00'

    # Set base URL
    base_url = 'http://www.ofertapune.com/jobfind.php?'

    # Set verification setting for certifiates of webpage. Check later also certification
    verification = True

    # Set data path
    listing_textfile_path = "C:\\Users\\Calogero\\Documents\\GitHub\\job_portal_scraper_simple_2\\data\\daily_scraping\\single_listings_htmls\\"

    # Create folder for listing output files
    time_folder = listing_textfile_path + now_str
    os.mkdir(time_folder)

    # Set max amount of pages to be crawled, 20 is default here
    maxpage = 1000000

    # Set maximum amount of repeats before ending
    max_repeats = 20

    # Execute functions for scraping
    start_time = time.time() # Capture start and end time for performance
    appended_data = scrape_ofertapune(maxpage, key_word, job_category, region, max_repeats, base_url, verification, now_str, listing_textfile_path)

    # Write output to Excel
    print("Writing to Excel file...")
    time.sleep(1)
    file_name = '_'.join(['C:\\Users\\Calogero\\Documents\\GitHub\\job_portal_scraper_simple_2\\data\\daily_scraping\\' +
    str(now_str), 'ofertapune.xlsx'])
    writer = pd.ExcelWriter(file_name, engine='xlsxwriter')
    appended_data.to_excel(writer, sheet_name = 'jobs')
    writer.save()
    workbook = writer.book
    worksheet = writer.sheets['jobs']
    format1 = workbook.add_format({'bold': False, "border" : True})
    worksheet.set_column('A:M', 15  , format1)
    writer.save()

    # Check end time
    end_time = time.time()
    duration = time.strftime("%H:%M:%S", time.gmtime(end_time - start_time))

    # For interaction and error handling
    final_text = "Your query was successful! Time elapsed:" + str(duration)
    print(final_text)
    time.sleep(0.5)

# Execute scraping
if __name__ == "__main__":
    main()





