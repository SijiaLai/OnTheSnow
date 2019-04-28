from requests import get
from requests.exceptions import RequestException
from contextlib import closing
from bs4 import BeautifulSoup
import pandas as pd
import numpy as np
import re
import time

def is_good_response(resp):
    """
    Ensures that the response is a html.
    """
    content_type = resp.headers['Content-Type'].lower()
    return (resp.status_code == 200 and
            content_type is not None
            and content_type.find('html') > -1)

def get_html_content(url):
    """
    Get the contents of the url.
    """
    time.sleep(1)
    try:
        with closing(get(url, stream=True)) as resp:
            content_type = resp.headers['Content-Type'].lower()
            if is_good_response(resp):
                return resp.content
            else:
                return None
    except RequestException as e:
        print('Error during requests to {0} : {1}'.format(url,str(e)))

def get_basic_resort_statistics(resortUrl):
    """
    Get basic stat for the ski resort.
    """
    resortContent = get_html_content(resortUrl)
    resortHtml = BeautifulSoup(resortContent, 'html.parser')

    stat = {}

    #Name
    if (resortHtml.find("span",{"class":"resort_name"}) != None):
        resortNameContent = resortHtml.find("span",{"class":"resort_name"})
        resortName = resortNameContent.get_text()
        stat["Name"] = resortName
    else:
        nameNote = "Check name info."
        stat["nameNote"] = nameNote

    #Locations
    if (resortHtml.find("p",{"class":"relatedRegions"})!= None):
        resortLocationContent = resortHtml.find("p",{"class":"relatedRegions"})
        resortLocationList = resortLocationContent.findAll("a")
        city = resortLocationList[0].get_text()
        state = resortLocationList[1].get_text()
        stat["city"] = city
        stat["state"] = state
    else:
        locationNote = "Check location info."
        stat["locationNote"] = locationNote

    #Elevation
    if (resortHtml.find("div",{"id":"resort_elevation"}) != None):
        elevationValues = resortHtml.findAll("div",{"class": "value"})
        keys = ["summit", "drop", "base"]
        for key, value in zip(keys,elevationValues):
            stat[key] = float(value.contents[0][:-2])
    else:
        elevationNote = "Check elevation information."
        stat["elevationNote"] = elevationNote
    
    #Lifts
    if (resortHtml.find("div",{"id":"resort_lifts"}) != None):
        
        liftValues = resortHtml.findAll("div",{"id":"resort_lifts"})
        
        tramsDescription = resortHtml.find("li",{"class":"trams"}).contents
        trams = float(tramsDescription[0])
        stat["trams"] = trams
        
        fastEightDescription = resortHtml.find("li",{"class":"fast_eight"}).contents
        if len(fastEightDescription) > 1:
            fastEight = float(fastEightDescription[0])
            stat["fastEight"] = fastEight
        else: 
            stat['fastEightNote'] = 'Check fast eight lift info.'
        
        fastSixesDescription = resortHtml.find("li",{"class":"fast_sixes"}).contents
        fastSixes = float(fastSixesDescription[0])
        stat["fastSixes"] = fastSixes
        fastQuadsDescription = resortHtml.find("li",{"class":"fast_quads"}).contents
        fastQuads = float(fastQuadsDescription[0])
        stat["fastQuads"] = fastQuads
        quadDescription = resortHtml.find("li",{"class":"quad"}).contents
        quad = float(quadDescription[0])
        stat["quad"] = quad
        tripleDescription = resortHtml.find("li",{"class":"triple"}).contents
        triple = float(tripleDescription[0])
        stat["triple"] = triple
        doubleDescription = resortHtml.find("li",{"class":"double"}).contents
        double = float(doubleDescription[0])
        stat["double"] = double
        surfaceDescription = resortHtml.find("li",{"class":"surface"}).contents
        surface = float(surfaceDescription[0])
        stat["surface"] = surface
        totalDescription = resortHtml.find("div",{"class":"liftTotal"}).contents
        total = float(totalDescription[0])
        stat["total"] = total
    else:
        liftNote = "Check lift information."
        stat["liftNote"] = liftNote

    #Terrain
    if (resortHtml.find("ul", {"class":"rt_trail diamonds"}) != None):
        terrainValues = resortHtml.findAll("p", {"class":["value", "label"]})
        terrainList = [i.contents for i in terrainValues]
        rawKeys =  terrainList[8::2]
        keys = [entry[0] for entry in rawKeys]
        rawValues = terrainList[9::2]
        values = [entry[0] for entry in rawValues]
        for key, value in zip(keys,values):
            stat[key] = float(value)
    else:
        terrainNote = "Check terrain information."
        stat["terrainNote"] = terrainNote

    #Days
    if (resortHtml.find("span", text="Days Open Last Year") != None):
        findDaysOpenLastYear = resortHtml.find("span",text="Days Open Last Year")
        daysOpenLastYear = float(findDaysOpenLastYear.find_next_sibling("strong").get_text())
        stat["daysOpenLastYear"] = daysOpenLastYear
    else:
        daysOpenNote = "Check days info"
        stat["daysOpenNote"] = daysOpenNote
    
    if (resortHtml.find("span", text="Projected Days Open") != None):
        findProjectedDaysOpen = resortHtml.find("span",text="Projected Days Open")
        projectedDaysOpen = float(findProjectedDaysOpen.find_next_sibling("strong").get_text())
        stat["projectedDaysOpen"] = projectedDaysOpen
    else:
        daysOpenNote = "Check days info"
        stat["daysOpenNote"] = daysOpenNote
    
    if (resortHtml.find("span", text="Years Open") != None):
        findYearsOpen = resortHtml.find("span",text="Years Open")
        yearsOpen = float(findYearsOpen.find_next_sibling("strong").get_text())
        stat["yearsOpen"] = yearsOpen
    else:
        yearOpenNote = "Check year open info"
        stat["yearOpenNote"] = yearOpenNote

    #Snow
    if (resortHtml.find("span", text="Average Snowfall") != None):
        findAverageSnowfall = resortHtml.find("span",text="Average Snowfall")
        averageSnowfall = float(findAverageSnowfall.find_next_sibling("strong").get_text()[:-1])
        stat["averageSnowfall"] = averageSnowfall
    else:
        snowfallNote = "Check snowfall info."
        stat["snowfallNote"] = snowfallNote

    return stat

def get_resort_prices(resortUrl):
    """
    Get the resort lift ticket prices.
    """
    prices = {}

    resortUrl = resortUrl[:-15] + "lift-tickets.html"
    priceContent = get_html_content(resortUrl)

    priceHtml = BeautifulSoup(priceContent, 'html.parser')
    priceData = priceHtml.findAll("span", {"class":["label", "value"]})
    priceDataList = [i.contents for i in priceData]

    #Adult Weekday Price
    ageLabelWeekday = str(priceDataList[4])
    rawValuesWeekday = priceDataList[5]
    if len(rawValuesWeekday) > 1:
        adultWeekdayPrice = float(rawValuesWeekday[1].get_text())
        prices["AdultWeekday"] = adultWeekdayPrice
    else:
        adultWeekdayPriceNote = "Check adult weekday price info."
        prices["adultWeekdayPriceNote"] = adultWeekdayPriceNote    
    
    #Adult Weekend Price
    ageLabelWeekend = str(priceDataList[12])
    rawValuesWeekend = priceDataList[13]
    if len(rawValuesWeekend) > 1:
        adultWeekendPrice = float(rawValuesWeekend[1].get_text())
        prices["AdultWeekend"] = adultWeekendPrice
    else:
        adultWeekendPriceNote = "Check adult weekday price info."
        prices["adultWeekendPriceNote"] = adultWeekendPriceNote   

    return prices

if __name__ == '__main__':
    """
    Extraxt data for each ski resort.
    """
    resortData = dict()

    with open('OnTheSnowUrls.csv','r') as f:
        csv_raw_cont=f.read()
    
    urlList=csv_raw_cont.split('\n')
    
    #Not sure how to fix the first url, thus starting on the second
    #Need to manually add the first one info
    urlList_Test = urlList[1::]

    for url in urlList_Test:

        resortUrl = url[8::]
        resortUrl="http://"+resortUrl
        print(resortUrl)
        stat = get_basic_resort_statistics(resortUrl)
        prices = get_resort_prices(resortUrl)
        time.sleep(1)
    
        resortData[url] = {**stat, **prices, "Url":resortUrl}

    df = pd.DataFrame.from_dict(resortData, orient = 'index')
    df.to_excel('OnTheSnow_v1.xlsx', sheet_name='sheet1',index=False)

    
