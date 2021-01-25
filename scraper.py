import re
import urllib.parse
import zlib
import requests

from bs4 import BeautifulSoup

def scraper(url, resp, crc_dict = {}, sim_hash = {}, global_url_lib = {},robot_dict = {},page_max_words = ("",0),fifty_common_words={},ics_subdomains = set()):
    links,crc,sim,updated_lib,robot_dict,max_words,common_words,subdomains = extract_next_links(url, resp,crc_dict,sim_hash, global_url_lib,robot_dict,page_max_words,fifty_common_words,ics_subdomains)

    return (links, crc, sim, updated_lib,robot_dict,max_words,common_words,subdomains)

def extract_next_links(url, resp, crc_dict = {},sim_hash = {},global_url_lib = {}, robot_dict = {},page_max_words=("",0),fifty_common_words={},ics_subdomains = set()):
    #Find out how many words are in the page 
    #Find out the top 50 common words as well excluding English stop words

    local_crc_dict = crc_dict
    local_sim_hash = sim_hash
    local_url_lib = global_url_lib
    url_list = {}
    subdomains = ics_subdomains
    

    base_url = urllib.parse.urlparse(url).netloc
    
    #Adding to the ics subdomains
    if "ics.uci.edu" in base_url:
        subdomains.add(base_url)

    #Avoid dead URLS that return a 200 status but no data 
    if resp.status == 200 and len(resp.raw_response.text) > 300:
        p = BeautifulSoup(resp.raw_response.text, "lxml")

        #Adds a crc element to the crc dict
        if local_crc_dict.get(zlib.crc32(bytes(p.get_text(),'utf-8'))) == None:
            local_crc_dict[zlib.crc32(bytes(p.get_text(),'utf-8'))] = True
        else:
            #print("There is an exact duplicate due from our crc check method")
            return ([],local_crc_dict,local_sim_hash, global_url_lib,robot_dict,page_max_words,fifty_common_words,subdomains)

        #Regex expression to get all of the strings I deem words
        assignment_1_re = re.compile(r"[a-zA-Z0-9]+")
        """
        There are some css text tags that are being picked up.
        """
        #Here is the fix sourced from StackOverflow: https://stackoverflow.com/questions/22799990/beatifulsoup4-get-text-still-has-javascript
        word_p = BeautifulSoup(resp.raw_response.text, "lxml")
        for script in word_p(["script", "style"]):
            script.decompose()
	
	#Container holding the parsed words from the site and their frequency
        dict_of_words = {}

        for word in re.split(r"[\s]+",word_p.get_text()):
            #Using the requirements of assignment one for tokenizing
            word = re.match(assignment_1_re, word)
            
            if word != None:
                word = word[0].lower()
                if dict_of_words.get(word) == None:
                    dict_of_words[word] = 1
                else:
                    dict_of_words[word] += 1

                if fifty_common_words.get(word) == None:
                    fifty_common_words[word] = 1
                else:
                    fifty_common_words[word] += 1

        if dict_of_words.get('') != None:
            del dict_of_words['']
        
        #This if statement replaces the old one if the current one is greater.
        if len(dict_of_words) > page_max_words[1]:
            page_max_words = (url, len(dict_of_words))

        if len(dict_of_words) < 100:
            local_url_lib[url] = True 
            return ([],local_crc_dict,local_sim_hash, local_url_lib,robot_dict,page_max_words,fifty_common_words,subdomains)

        #We will also use the dict of words for similar duplicate detections since it has the frequency of the words
        #We will be using simhash
        
        #This is the fingerprint part
        #Hash constant 
        hash_constant = 31

        #Vector V
        vector_v = [0] * 32

        #Hash table
        hash_table = {}
        
        for word in dict_of_words:
            hash_value = 0
            #Using this string polynomial method as it considers the position of the chars as well
            #Learned from ICS 46
            for char in range(len(word)):
                hash_value += (ord(word[char]) * (hash_constant ** char))

            #binary representation of a number up to 14 positions
            binary_32 = bin(hash_value % 2**32)[2:]

            if len(binary_32) != 14:
                binary_32 = '0'*(32-len(binary_32)) + binary_32

            hash_table[word] = binary_32
        
        for bin_word, binary in hash_table.items():
            for bin_index in range(len(binary)):
                if binary[bin_index] == '1':
                    vector_v[bin_index] += dict_of_words[bin_word]
                else:
                    vector_v[bin_index] -= dict_of_words[bin_word]
        
    

        fourteen_bit_fingerprint = ""
        for number in vector_v:
            if number > 0 :
                fourteen_bit_fingerprint += '1'
            else:
                fourteen_bit_fingerprint += '0'

        if local_sim_hash.get(fourteen_bit_fingerprint) == None:
            local_sim_hash[fourteen_bit_fingerprint] = True
        else:
            #print("There was a similarity detection from a sim-hash method")
            return ([],local_crc_dict,local_sim_hash,global_url_lib,robot_dict,page_max_words,fifty_common_words,subdomains)

        if robot_dict.get(base_url) == None:
            #Do this only once 
            #Politeness Feature of following the robots.txt 
            robot_link = urllib.parse.urljoin(url, "/robots.txt")
            robot_request = requests.get(robot_link, timeout=5)

            #Thinking about making this global variable or somewhat permanent
            disallowed_sites = []
            sitemap_sites = []

            disallowStr = re.compile(r"\s?(Disallow:\s\S*)")
            sitemapStr = re.compile(r"\s?(Sitemap:\s\S+)")
            
            #Finds and compiles the websites that are disallowed by robots.txt and appends it to a list
            if robot_request.status_code == 200:
                robot_soup = BeautifulSoup(robot_request.text, "lxml")
                for word in robot_soup.stripped_strings:
                    #Captures disallowed links
                    for dis_string in re.split(disallowStr,word):
                        if re.match(disallowStr,dis_string):
                            disallowed_sites.append(urllib.parse.urljoin(url, dis_string[11:]))
                    #Captures sitemaps links        
                    for site_string in re.split(sitemapStr,word):
                        if re.match(sitemapStr,site_string):
                            sitemap_sites.append(site_string[9:]) 
            robot_dict[base_url] = (disallowed_sites,sitemap_sites)

        previous_url = None

        for link in p.find_all('a'):
            found_link = link.get("href")
            #Makes the link absolute
            found_link = urllib.parse.urljoin(url, found_link)
            #Takes away the fragments 
            found_link = urllib.parse.urlparse(found_link) 
            found_link = found_link[0] + "://" + found_link[1] + found_link[2] + found_link[3] + found_link[4]


            #If we find a url that is disallowed by the robots.txt, pass it 
            if found_link in robot_dict[base_url][0]:
                continue
            #Or thats already in the list
            elif url_list.get(found_link) != None:
                continue
            elif local_url_lib.get(found_link) != None:
                continue
            
    
            url_list[found_link] =  is_valid(found_link,previous_url)
            previous_url = found_link
            local_url_lib[found_link] = url_list[found_link]

        #Adds in the sitemaps_to the url_list that would be returned 
        for sitemap in robot_dict[base_url][1]:
            url_list[sitemap] = is_valid(sitemap)

    for link in url_list.copy():
        if url_list[link] == False:
            del url_list[link]


    return (list(url_list.keys()), crc_dict, sim_hash, local_url_lib,robot_dict,page_max_words,fifty_common_words,subdomains)

def str_intersection(first_string, second_string):
    """
    Returns the length of intersection of two strings
    """

    #Normalizing both schemes so that it doesn't intefere with this
    if "https" not in  first_string:
        first_string = "https" + first_string[4:]

    if "https" not in second_string:
        second_string = "https" + second_string[4:]

    string_index = 0
    diff_amount = 0
    while string_index < len(first_string) and string_index < len(second_string):
        if first_string[string_index] != second_string[string_index]:
            diff_amount += 1
        string_index += 1
    
    if string_index > len(first_string) and string_index > len(second_string):
        return diff_amount
    elif string_index > len(first_string):
        diff_amount += len(second_string[string_index:])
    else:
        diff_amount += len(first_string[string_index:])

    return diff_amount

def is_valid(to_be_checked_url, previous_url = None):

    these_domains_only = [
    "ics.uci.edu",
    "cs.uci.edu",
    "informatics.uci.edu",
    "stat.uci.edu"
    ]

    #How many subdomains did you find in in the ics.uci.edu domain? 
    #Submit the list of subdomains ordered alphabetically and the number of unique pages detected in each subdomain. 
    try:
        parsed = urllib.parse.urlparse(to_be_checked_url)    
        
        if parsed.scheme not in set(["http", "https"]):
            #print(f"{parsed.scheme} is not in set [http, https]")
            return False

        if "http://sconce.ics.uci.edu" in to_be_checked_url:
            #print("http://sconce.ics.uci.edu is in the url")
            return False
        
        if "https://www.informatics.uci.edu/files/pdf/InformaticsBrochure-March2018" in to_be_checked_url:
            return False
        elif "http://www.informatics.uci.edu/files/pdf/InformaticsBrochure-March2018" in to_be_checked_url:
            return False
            
        #This for some reason returns true due to the end of the ics email to avoid this crawler trap
        if "mailto" in to_be_checked_url:
            #print("This one has to mailto in it: ",to_be_checked_url)
            return False
        
        #Some hard-coded paths to avoid crawler traps
        if re.match("^/~[a-zA-Z]+", parsed.path) != None:
            #print("Useless information from professors: ", parsed.path)
            return False
        
        #Some hard-coded netloc to prevent eecs being mixed with cs
        if "eecs.uci.edu" in parsed.netloc:
            #print("eecc.uci.edu is in the netloc")
            return False

        #Some more hard-code to avoid useless info/ web trap
        if "/faculty/profiles/view_faculty.phpucinetid=" in parsed.path:
            #print("Useless info: ", parsed.path)
            return False
        
        #If the intersection of two strings is minimal meaning they are about the same, return False 
        #Avoiding those little changes in the url crawler traps
        if previous_url != None and str_intersection(to_be_checked_url, previous_url) < 6:
            #print("Quite similar paths: {og_url} vs {previous_url}".format(og_url=to_be_checked_url, previous_url = previous_url))
            return False

        only_domains_bool = False
        if parsed.path == "/department/information_computer_sciences" and parsed.netloc == "today.uci.edu":
                only_domains_bool = True
        else:
            for only_domain in these_domains_only:
                if only_domain in parsed.netloc:
                    only_domains_bool = True

        matched_bool = not re.match(
            r".*\.(css|js|bmp|gif|jpe?g|ico"
            + r"|png|tiff?|mid|mp2|mp3|mp4"
            + r"|wav|avi|mov|mpeg|ram|m4v|mkv|ogg|ogv|pdf"
            + r"|ps|eps|tex|ppt|pptx|doc|docx|xls|xlsx|names"
            + r"|data|dat|exe|bz2|tar|msi|bin|7z|psd|dmg|iso"
            + r"|epub|dll|cnf|tgz|sha1"
            + r"|thmx|mso|arff|rtf|jar|csv"
            + r"|rm|smil|wmv|swf|wma|zip|rar|gz)$", parsed.path.lower())

        if matched_bool and only_domains_bool:
            return True
        
        return False

    except TypeError:
        print ("TypeError for ", parsed)
        raise
