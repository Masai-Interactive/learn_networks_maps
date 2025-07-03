import sys
import os
import csv
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

def scrape_incs_schools_selenium():
    url = "https://www.incschools.org/find-a-charter-school/"

    chrome_options = Options()
    chrome_options.add_argument("--headless")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")

    driver = webdriver.Chrome(options=chrome_options)
    driver.get(url)

    # Wait until at least one school result appears
    try:
        WebDriverWait(driver, 20).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, ".schools-list li"))
        )
    except Exception as e:
        driver.quit()
        raise RuntimeError("Could not find schools list after waiting: " + str(e))

    # Get fully rendered HTML
    html = driver.page_source
    driver.quit()

    soup = BeautifulSoup(html, 'html.parser')
    items = soup.select('.schools-list li')

    schools = []
    for li in items:
        a_tag = li.find('a')
        name = a_tag.get_text(strip=True) if a_tag else ''
        link = a_tag['href'] if a_tag and a_tag.has_attr('href') else ''

        address = li.find('div', class_='address')
        phone = li.find('div', class_='phone')
        grades = li.find('div', class_='grades')
        charter = li.find('div', class_='charter')
        network = li.find('div', class_='network')

        schools.append({
            'name': name,
            'link': link,
            'address': address.get_text(strip=True) if address else '',
            'phone': phone.get_text(strip=True) if phone else '',
            'grades': grades.get_text(strip=True).replace('Grades Served:', '').strip() if grades else '',
            'charter': charter.get_text(strip=True).replace('Charter Type:', '').strip() if charter else '',
            'network': network.get_text(strip=True).replace('Network:', '').strip() if network else ''
        })

    return schools

def write_schools_to_csv(schools, output_path):
    fieldnames = ['name', 'link', 'address', 'phone', 'grades', 'charter', 'network']

    with open(output_path, mode='w', newline='', encoding='utf-8') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        for school in schools:
            writer.writerow(school)

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python scrape_incs.py <output_csv_path>")
        sys.exit(1)

    output_csv = sys.argv[1]
    os.makedirs(os.path.dirname(output_csv), exist_ok=True)

    schools = scrape_incs_schools_selenium()
    write_schools_to_csv(schools, output_csv)

    print(f"✅ Saved {len(schools)} schools to {output_csv}")
