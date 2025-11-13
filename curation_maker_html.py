import requests
import time
from typing import List
from bs4 import BeautifulSoup
import re


def scrape_and_filter_by_keywords(url: str, keywords: List[str], debug: bool = False):
    """
    Scrape Tata CLiQ Luxury by fetching HTML pages directly
    Fast and gets ALL product content
    """
    print(f"\n{'='*60}")
    print(f"🚀 CURATION MAKER - HTML Scraping")
    print(f"URL: {url}")
    print(f"Keywords: {', '.join(keywords)}")
    print(f"{'='*60}\n")

    # Convert keywords to lowercase for case-insensitive matching
    keywords_lower = [kw.lower() for kw in keywords]

    headers = {
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
        'Accept-Language': 'en-US,en;q=0.9',
        'Referer': 'https://luxury.tatacliq.com/'
    }

    # Step 1: Extract search query from URL
    search_text = extract_search_text(url)
    print(f"Search query: {search_text}\n")

    # Step 2: Get ALL products using pagination
    print("="*60)
    print("STEP 1: Getting all products")
    print("="*60)
    
    all_products = []
    page = 0
    
    while page < 100:  # Safety limit
        print(f"Fetching page {page}...", end=" ")
        
        api_url = build_api_url(search_text, page)
        
        try:
            response = requests.get(api_url, headers=headers, timeout=10)
            
            if response.status_code != 200:
                print(f"❌ Failed (status {response.status_code})")
                break
            
            data = response.json()
            products = data.get('searchresult', [])
            
            if not products:
                print("✓ No more products")
                break
            
            # Extract MP codes and product URLs
            for product in products:
                mp_code = product.get('productId', '')
                if mp_code:
                    # Build product URL
                    web_url = product.get('webURL', '')
                    if not web_url.startswith('http'):
                        web_url = f"https://luxury.tatacliq.com{web_url}"
                    
                    all_products.append({
                        'mp_code': mp_code,
                        'url': web_url,
                        'title': product.get('productname', ''),
                        'brand': product.get('brandname', ''),
                    })
            
            print(f"✓ Found {len(products)} products (total: {len(all_products)})")
            
            if len(products) < 24:
                print("  → Last page reached")
                break
            
            page += 1
            time.sleep(0.5)
            
        except Exception as e:
            print(f"❌ Error: {e}")
            break
    
    print(f"\n✓ Total products found: {len(all_products)}")
    
    # Step 3: Scrape HTML and filter by keywords
    print(f"\n{'='*60}")
    print(f"STEP 2: Scraping product pages and filtering")
    print(f"{'='*60}\n")
    
    filtered_mp_codes = []
    
    for i, product in enumerate(all_products, 1):
        mp_code = product['mp_code']
        product_url = product['url']
        
        print(f"Checking {i}/{len(all_products)}: {mp_code}...", end=" ")
        
        try:
            # Fetch the product HTML page
            response = requests.get(product_url, headers=headers, timeout=15)
            
            if response.status_code != 200:
                print(f"❌ Failed to load (status {response.status_code})")
                continue
            
            # Parse HTML
            soup = BeautifulSoup(response.text, 'html.parser')
            
            # Extract all text content
            searchable_parts = []
            
            # 1. Title and brand (from listing)
            searchable_parts.append(product['title'])
            searchable_parts.append(product['brand'])
            
            # 2. Meta description (often contains the "WHY IT'S SPECIAL" text)
            meta_desc = soup.find('meta', {'name': 'description'})
            if meta_desc and meta_desc.get('content'):
                searchable_parts.append(meta_desc['content'])
            
            # 3. Meta keywords
            meta_keywords = soup.find('meta', {'name': 'keywords'})
            if meta_keywords and meta_keywords.get('content'):
                searchable_parts.append(meta_keywords['content'])
            
            # 4. Get ALL text from the HTML including hidden sections
            # Don't just get visible text - parse everything
            
            # Method 1: Get all text including hidden elements
            all_text = soup.get_text(separator=' ', strip=True)
            searchable_parts.append(all_text)
            
            # Method 2: Look for specific feature sections
            # Features might be in divs, tables, or lists even if hidden
            # Look for common feature keywords and grab their parent elements
            feature_keywords = ['sleeve', 'pattern', 'fit', 'neck', 'collar', 'fabric', 
                              'color', 'colour', 'wash', 'length', 'occasion', 'style',
                              'material', 'composition', 'care', 'model']
            
            for keyword in feature_keywords:
                # Find all elements containing these words
                elements = soup.find_all(string=re.compile(keyword, re.IGNORECASE))
                for elem in elements:
                    if elem.parent:
                        # Get the parent and next siblings (feature name + value)
                        parent_text = elem.parent.get_text(separator=' ', strip=True)
                        searchable_parts.append(parent_text)
                        
                        # Also check next sibling for the value
                        if elem.parent.next_sibling:
                            sibling_text = elem.parent.next_sibling.get_text(separator=' ', strip=True) if hasattr(elem.parent.next_sibling, 'get_text') else str(elem.parent.next_sibling)
                            searchable_parts.append(sibling_text)
            
            # Combine all text
            searchable_text = ' '.join(str(p) for p in searchable_parts if p).lower()
            
            # Debug output
            if debug and i <= 3:
                print(f"\n{'='*60}")
                print(f"DEBUG Product {i}")
                print(f"MP Code: {mp_code}")
                print(f"URL: {product_url}")
                print(f"Title: {product['title']}")
                print(f"Meta description: {meta_desc['content'][:200] if meta_desc else 'None'}...")
                print(f"Total searchable text length: {len(searchable_text)} chars")
                print(f"\nSearching for these keywords: {keywords_lower}")
                print(f"\nFirst 800 chars of searchable text:")
                print(searchable_text[:800])
                print(f"\n...Last 400 chars:")
                print(searchable_text[-400:])
                print(f"{'='*60}\n")
                print(f"Checking {i}/{len(all_products)}: {mp_code}...", end=" ")
            
            # Check for keyword matches
            matches = []
            for keyword in keywords_lower:
                if keyword in searchable_text:
                    matches.append(keyword)
            
            if matches:
                filtered_mp_codes.append(mp_code)
                print(f"✓ MATCHED ({', '.join(matches)})")
            else:
                print("⊘ No match")
            
            # Rate limiting - be respectful
            time.sleep(0.5)
            
        except Exception as e:
            print(f"❌ Error: {str(e)[:40]}")
            continue
    
    print(f"\n{'='*60}")
    print(f"FILTERING COMPLETE")
    print(f"{'='*60}")
    print(f"Products checked: {len(all_products)}")
    print(f"Products matched: {len(filtered_mp_codes)}")
    if len(all_products) > 0:
        print(f"Match rate: {len(filtered_mp_codes)/len(all_products)*100:.1f}%")
    
    return filtered_mp_codes


def extract_search_text(url: str) -> str:
    """Extract searchText parameter from URL"""
    match = re.search(r'[?&](?:q|searchText)=([^&]+)', url)
    if match:
        return match.group(1)
    return ':relevance:category:LSH1110101:inStockFlag:true'


def build_api_url(search_text: str, page: int) -> str:
    """Build the Tata CLiQ search API URL"""
    base_url = "https://searchbff.tatacliq.com/products/lux/search"
    params = {
        'pageSize': 24,
        'page': page,
        'searchText': search_text,
        'isPwa': 'true',
        'channel': 'web',
        'isMDE': 'true'
    }
    param_str = '&'.join([f"{k}={v}" for k, v in params.items()])
    return f"{base_url}?{param_str}"


def save_mp_codes(mp_codes: List[str], filename: str = "filtered_mp_codes.txt"):
    """Save MP codes to file"""
    with open(filename, 'w', encoding='utf-8') as f:
        for mp_code in mp_codes:
            f.write(f"{mp_code}\n")
    print(f"\n✓ Saved {len(mp_codes)} MP codes to {filename}")


def main():
    """
    HTML SCRAPING CURATION MAKER - Multiple Curations
    
    Scrapes once, filters multiple times
    """
    
    print("\n" + "="*60)
    print("CURATION MAKER - Multiple Curations")
    print("="*60)
    print("\nThis tool will:")
    print("  1. Get all products from the listing page (API)")
    print("  2. Scrape each product's HTML page")
    print("  3. Extract ALL text (description, features, etc.)")
    print("  4. Filter by multiple keyword groups")
    print("  5. Save all curations to one file")
    print("\n✅ Scrape once, create multiple curations!")
    print("⏱️  Takes ~0.5 seconds per product")
    print("="*60 + "\n")

    # Get URL
    url = input("Enter Tata CLiQ Luxury URL: ").strip()
    
    if not url:
        print("Using default dresses URL...")
        url = "https://luxury.tatacliq.com/luxury/c-lsh1110101?q=%3Arelevance%3Acategory%3ALSH1110101%3AinStockFlag%3Atrue"

    # Get multiple curations
    print("\nEnter your curations (one per line)")
    print("Format: Curation Name | keyword1, keyword2, keyword3")
    print("Example: Party Wear | embellished, sequin, beaded")
    print("Type 'done' when finished\n")

    curations = []
    while True:
        line = input(f"Curation {len(curations)+1}: ").strip()
        if line.lower() == 'done':
            break
        if '|' not in line:
            print("  ⚠️ Use format: Name | keyword1, keyword2")
            continue
        
        name, keywords_str = line.split('|', 1)
        keywords = [kw.strip() for kw in keywords_str.split(',') if kw.strip()]
        
        if name.strip() and keywords:
            curations.append({
                'name': name.strip(),
                'keywords': keywords
            })
            print(f"  ✓ Added: {name.strip()} ({len(keywords)} keywords)")

    if not curations:
        print("\n❌ No curations provided!")
        return

    # Ask for debug mode
    debug = input("\nShow detailed text for first 3 products? (y/n): ").strip().lower() == 'y'
    
    print(f"\n📋 You have {len(curations)} curations:")
    for c in curations:
        print(f"  • {c['name']}: {', '.join(c['keywords'])}")

    input("\nPress Enter to start scraping...")

    # Scrape all products once
    start_time = time.time()
    all_products_data = scrape_all_products(url, debug)
    scrape_time = time.time() - start_time

    if not all_products_data:
        print("\n❌ No products scraped!")
        return

    print(f"\n✓ Scraped {len(all_products_data)} products in {scrape_time/60:.1f} minutes")

    # Filter for each curation
    print(f"\n{'='*60}")
    print(f"FILTERING BY KEYWORDS")
    print(f"{'='*60}\n")
    
    results = {}
    for curation in curations:
        print(f"Filtering: {curation['name']}...", end=" ")
        matched = filter_by_keywords(all_products_data, curation['keywords'])
        results[curation['name']] = matched
        print(f"✓ {len(matched)} matches")

    # Save all results
    save_multiple_curations(results, "all_curations.txt")

    # Print summary
    print(f"\n{'='*60}")
    print(f"SUMMARY")
    print(f"{'='*60}")
    total_time = time.time() - start_time
    print(f"Total time: {total_time/60:.1f} minutes")
    print(f"\nResults:")
    for name, mp_codes in results.items():
        print(f"  {name:30s} - {len(mp_codes):4d} products")
    
    print(f"\n🎉 Done! Check 'all_curations.txt'")


def scrape_all_products(url: str, debug: bool = False):
    """
    Scrape all products and return their data with searchable text
    """
    headers = {
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
        'Accept-Language': 'en-US,en;q=0.9',
        'Referer': 'https://luxury.tatacliq.com/'
    }

    # Step 1: Get all product URLs
    search_text = extract_search_text(url)
    print(f"Search query: {search_text}\n")
    
    print("="*60)
    print("STEP 1: Getting all products")
    print("="*60)
    
    all_products = []
    page = 0
    
    while page < 500:
        print(f"Fetching page {page}...", end=" ")
        
        api_url = build_api_url(search_text, page)
        
        try:
            response = requests.get(api_url, headers=headers, timeout=10)
            
            if response.status_code != 200:
                break
            
            data = response.json()
            products = data.get('searchresult', [])
            
            if not products:
                break
            
            for product in products:
                mp_code = product.get('productId', '')
                if mp_code:
                    web_url = product.get('webURL', '')
                    if not web_url.startswith('http'):
                        web_url = f"https://luxury.tatacliq.com{web_url}"
                    
                    all_products.append({
                        'mp_code': mp_code,
                        'url': web_url,
                        'title': product.get('productname', ''),
                        'brand': product.get('brandname', ''),
                    })
            
            print(f"✓ Found {len(products)} products (total: {len(all_products)})")
            
            if len(products) < 24:
                break
            
            page += 1
            time.sleep(0.5)
            
        except Exception as e:
            print(f"❌ Error: {e}")
            break
    
    print(f"\n✓ Total products found: {len(all_products)}")
    
    # Step 2: Scrape all product pages
    print(f"\n{'='*60}")
    print(f"STEP 2: Scraping product pages")
    print(f"{'='*60}\n")
    
    products_with_text = []
    
    for i, product in enumerate(all_products, 1):
        mp_code = product['mp_code']
        product_url = product['url']
        
        print(f"Scraping {i}/{len(all_products)}: {mp_code}...", end=" ")
        
        try:
            response = requests.get(product_url, headers=headers, timeout=15)
            
            if response.status_code != 200:
                print(f"❌ Failed")
                continue
            
            soup = BeautifulSoup(response.text, 'html.parser')
            
            searchable_parts = []
            
            # Extract content
            searchable_parts.append(product['title'])
            searchable_parts.append(product['brand'])
            
            meta_desc = soup.find('meta', {'name': 'description'})
            if meta_desc and meta_desc.get('content'):
                searchable_parts.append(meta_desc['content'])
            
            meta_keywords = soup.find('meta', {'name': 'keywords'})
            if meta_keywords and meta_keywords.get('content'):
                searchable_parts.append(meta_keywords['content'])
            
            # Targeted sections only
            product_sections = soup.find_all(['h1', 'h2', 'h3', 'p', 'div'], 
                                             class_=re.compile(r'product|description|detail|feature|special', re.IGNORECASE))
            
            for section in product_sections:
                section_str = str(section.get('class', [])) + str(section.get('id', ''))
                if any(word in section_str.lower() for word in ['similar', 'viewed', 'recommend', 'related', 'carousel']):
                    continue
                
                text = section.get_text(separator=' ', strip=True)
                searchable_parts.append(text)
            
            searchable_text = ' '.join(str(p) for p in searchable_parts if p).lower()
            
            products_with_text.append({
                'mp_code': mp_code,
                'searchable_text': searchable_text,
                'title': product['title'],
                'brand': product['brand']
            })
            
            print(f"✓ ({len(searchable_text)} chars)")
            
            # Show debug for first 3
            if debug and i <= 3:
                print(f"\n{'='*60}")
                print(f"DEBUG Product {i}")
                print(f"MP Code: {mp_code}")
                print(f"Searchable text length: {len(searchable_text)} chars")
                print(f"First 500 chars: {searchable_text[:500]}")
                print(f"{'='*60}\n")
            
            time.sleep(0.5)
            
        except Exception as e:
            print(f"❌ Error: {str(e)[:30]}")
            continue
    
    return products_with_text


def filter_by_keywords(products_data: List, keywords: List[str]) -> List[str]:
    """
    Filter products by keywords
    """
    keywords_lower = [kw.lower() for kw in keywords]
    matched_mp_codes = []
    
    for product in products_data:
        matches = []
        for keyword in keywords_lower:
            if keyword in product['searchable_text']:
                matches.append(keyword)
        
        if matches:
            matched_mp_codes.append(product['mp_code'])
    
    return matched_mp_codes


def save_multiple_curations(results, filename: str = "all_curations.txt"):
    """
    Save multiple curations to one file
    """
    with open(filename, 'w', encoding='utf-8') as f:
        f.write(f"CURATION RESULTS\n")
        f.write(f"Generated: {time.strftime('%Y-%m-%d %H:%M:%S')}\n")
        f.write(f"{'='*60}\n\n")
        
        for curation_name, mp_codes in results.items():
            f.write(f"\n{'='*60}\n")
            f.write(f"CURATION: {curation_name}\n")
            f.write(f"Total Products: {len(mp_codes)}\n")
            f.write(f"{'='*60}\n\n")
            
            for mp_code in mp_codes:
                f.write(f"{mp_code}\n")
            
            f.write(f"\n")
    
    print(f"\n✓ Saved all curations to {filename}")


if __name__ == "__main__":
    main()
