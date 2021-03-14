import json
import time
from urllib import request
from urllib.error import HTTPError, URLError
from bs4 import BeautifulSoup
import random
import sys
from os import path
import argparse
from multiprocessing import Pool, cpu_count

import config
sys.path.append(config.path_scrapers)
from recipe_scrapers import scrap_me

HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows; U; Windows NT 5.1; en-US; rv:1.9.0.7) Gecko/2009021910 Firefox/3.0.7'
}

def get_recipe(url):
    try:
        scrap = scrap_me(url)
    except:
        print('Could not scrape URL {}'.format(url))
        return {}

    try:
        title = scrap.title()
    except AttributeError:
        title = None

    try:
        ingredients = scrap.ingredients()
    except AttributeError:
        ingredients = None

    try:
        instructions = scrap.instructions()
    except AttributeError:
        instructions = None

    try:
        picture_link = scrap.picture()
    except AttributeError:
        picture_link = None

    try:
        fat_content = scrap.fatContent()
    except AttributeError:
        fat_content = None

    try:
        satfat_content = scrap.satfatContent()
    except AttributeError:
        satfat_content = None

    # try:
    #     pufa_content = scrap.pufaContent()
    # except AttributeError:
    #     pufa_content = None
    #
    # try:
    #     mufa_content = scrap.mufaContent()
    # except AttributeError:
    #     mufa_content = None

    try:
        sodium_content = scrap.sodiumContent()
    except AttributeError:
        sodium_content = None

    try:
        carb_content = scrap.carbContent()
    except AttributeError:
        carb_content = None

    try:
        protein_content = scrap.proteinContent()
    except AttributeError:
        protein_content = None

    try:
        fiber_content = scrap.fiberContent()
    except AttributeError:
        fiber_content = None

    try:
        calories = scrap.calories()
    except AttributeError:
        calories = None

    try:
        tag_cuisine = scrap.tagCuisine()
    except AttributeError:
        tag_cuisine = None

    try:
        tag_special = scrap.tagSpecial()
    except AttributeError:
        tag_special = None

    try:
        tag_meal = scrap.tagMeal()
    except AttributeError:
        tag_meal = None

    try:
        tag_tag = scrap.tagTag()
    except AttributeError:
        tag_tag = None

    try:
        tag_ingre = scrap.tagIngre()
    except AttributeError:
        tag_ingre = None

    try:
        tag_type = scrap.tagType()
    except AttributeError:
        tag_type = None

    try:
        tag_occasion = scrap.tagOccasion()
    except AttributeError:
        tag_occasion = None

    try:
        tag_tech = scrap.tagTech()
    except AttributeError:
        tag_tech = None

    try:
        tag_equip = scrap.tagEquip()
    except AttributeError:
        tag_equip = None

    try:
        tag_source = scrap.tagSource()
    except AttributeError:
        tag_source = None

    try:
        total_time = scrap.totalTime()
    except AttributeError:
        total_time = None


    return {
        'title': title,
        'ingredients': ingredients,
        'instructions': instructions,
        'picture_link': picture_link,
        'fat_content' : fat_content,
        'satfat_content': satfat_content,
        # 'pufa_content' : pufa_content,
        # 'mufa_content' : mufa_content,
        'sodium_content': sodium_content,
        'carb_content': carb_content,
        'protein_content': protein_content,
        'fiber_content': fiber_content,
        'calories'    : calories,
        'tag_cuisine' : tag_cuisine,
        'tag_special' : tag_special,
        'tag_meal'    : tag_meal,
        'tag_tag'     : tag_tag,
        'tag_ingredient': tag_ingre,
        'tag_type'    : tag_type,
        'tag_occasion': tag_occasion,
        'tag_tech'    : tag_tech,
        'tag_equipment': tag_equip,
        'tag_source'  : tag_source,
        'total_time'  : total_time
    }

def get_all_recipes_fn(page_str, page_num):
    base_url = 'http://www.foodnetwork.com'
    search_url_str = 'recipes/a-z'
    url = '{}/{}/{}/p/{}'.format(base_url, search_url_str, page_str, page_num)

    try:
        soup = BeautifulSoup(request.urlopen(
            request.Request(url, headers=HEADERS)).read(), "html.parser")
        recipe_link_items = soup.select('div.o-Capsule__m-Body ul.m-PromoList li a')
        recipe_links = [r.attrs['href']for r in recipe_link_items]
        print('Read {} recipe links from {}'.format(len(recipe_links), url))
        return recipe_links
    except (HTTPError, URLError):
        print('Could not parse page {}'.format(url))
        return []


def get_all_recipes_ar(page_num):
    base_url = 'http://allrecipes.com'
    search_url_str = 'recipes/?page'
    url = '{}/{}={}'.format(base_url, search_url_str, page_num)

    try:
        soup = BeautifulSoup(request.urlopen(
            request.Request(url, headers=HEADERS)).read(), "html.parser")
        recipe_link_items = soup.select('article > a:nth-of-type(1)')
        #recipe_link_items = soup.select('article.fixed-recipe-card div.fixed-recipe-card__h3 a.view-complete-item')
        recipe_links = list(set(
            [r['href'] for r in recipe_link_items
             if r is not None and r['href'].split('/')[1] == 'recipe']))
        return {base_url + r: get_recipe(base_url + r) for r in recipe_links}
    except ConnectionResetError:
        print('connection reset by peer')
    except (HTTPError, URLError):
        print('Could not parse page {}'.format(url))
        return []


def get_all_recipes_epi(page_num):
    base_url = 'http://www.epicurious.com'
    search_url_str = 'search/?content=recipe&page'
    url = '{}/{}={}'.format(base_url, search_url_str, page_num)

    try:
        soup = BeautifulSoup(request.urlopen(
            request.Request(url, headers=HEADERS)).read(), "html.parser")
        recipe_link_items = soup.select('div.results-group article.recipe-content-card a.view-complete-item')
        recipe_links = [r['href'] for r in recipe_link_items]
        return {base_url + r: get_recipe(base_url + r) for r in recipe_links}
    except (HTTPError, URLError):
        print('Could not parse page {}'.format(url))
        return []


def scrape_recipe_box(scraper, site_str, page_iter, status_interval=50):

    if args.append:
        recipes = quick_load(site_str)
    else:
        recipes = {}
    start = time.time()
    if args.multi:
        pool = Pool(cpu_count() * 2)
        results = pool.map(scraper, page_iter)
        for r in results:
            recipes.update(r)
    else:
        for i in page_iter:
            recipes.update(scraper(i))
            if i % status_interval == 0:
                print('Scraping page {} of {}'.format(i, max(page_iter)))
                quick_save(site_str, recipes)
            time.sleep(.3+args.sleep * random.random())

    print('Scraped {} recipes from {} in {:.0f} minutes'.format(
        len(recipes), site_str, (time.time() - start) / 60))
    quick_save(site_str, recipes)


def get_fn_letter_links():
    # get list of pages with links to recipes
    base_url = 'http://www.foodnetwork.com'
    search_url_str = 'recipes/a-z'
    url = '{}/{}/{}'.format(base_url, search_url_str, '')

    try:
        soup = BeautifulSoup(request.urlopen(
            request.Request(url, headers=HEADERS)).read(), "html.parser")
        page_link_items = soup.select('ul.o-IndexPagination__m-List li a')
        letter_links = [p['href'] for p in page_link_items]
        return letter_links
    except (HTTPError, URLError):
        print('Could not parse page {}'.format(url))

def get_fn_recipe_links():

    letter_links = get_fn_letter_links()
    recipe_links = {}
    page_tracker = 0

    for page in letter_links:
        recipe_set = True
        page_num = 1
        lag0 = 0
        while recipe_set:
            t0 = time.time()
            recipe_set = get_all_recipes_fn(path.basename(page), page_num)
            lag1 = time.time() - t0
            recipe_links[page_tracker] = []
            recipe_links[page_tracker].extend(recipe_set)
            page_num += 1
            page_tracker += 1
            time.sleep(lag1 * .5 + lag0 * .5)
            lag0 = lag1

    return recipe_links

def scrape_fn(page_num):
    global recipe_links_dict
    recipe_links = recipe_links_dict[page_num]
    return {r: get_recipe(r) for r in recipe_links}

def quick_load(site_str):
    return load_recipes(path.join(
        config.path_data, 'recipes_raw_{}.json'.format(site_str)))

def load_recipes(filename):
    with open(filename, 'r') as f:
        return json.load(f)

def quick_save(site_str, recipes):
    save_recipes(
        path.join(config.path_data, 'recipes_raw_{}.json'.format(site_str)),
        recipes)

def save_recipes(filename, recipes):
    with open(filename, 'w') as f:
        json.dump(recipes, f)

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--fn', action='store_true', help='Food Network')
    parser.add_argument('--epi', action='store_true', help='Epicurious')
    parser.add_argument('--ar', action='store_true', help='All Recipes')
    parser.add_argument('--multi', action='store_true', help='Multi threading')
    parser.add_argument('--append', action='store_true',
                        help='Append scrapping run to existing JSON doc')
    parser.add_argument('--status', type=int, default=50, help='Print status interval')
    parser.add_argument('--start', type=int, default=1, help='Start page')
    parser.add_argument('--pages', type=int, default=3000, help='Number of pages to scrape')
    parser.add_argument('--sleep', type=int, default=0,
                        help='Seconds to wait before scraping next page')
    args = parser.parse_args()
    if args.fn:
        recipe_links_dict = get_fn_recipe_links()
        page_iter = range(1, len(recipe_links_dict) + 1)
        scrape_recipe_box(scrape_fn, 'fn', page_iter, args.status)
    if args.epi:
        page_iter = range(args.start, args.pages + args.start)
        scrape_recipe_box(get_all_recipes_epi, 'epi', page_iter, args.status)
    if args.ar:
        page_iter = range(args.start, args.pages + args.start)
        scrape_recipe_box(get_all_recipes_ar, 'ar', page_iter, args.status)
