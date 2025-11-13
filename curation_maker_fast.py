import asyncio
import aiohttp
import time
from typing import List, Dict
from bs4 import BeautifulSoup
import re


async def scrape_all_products_async(url: str, debug: bool = False):
    """
    Scrape all products using async for massive speed improvement
    """
    print(f"\n{'='*60}")
    print(f"🚀 ASYNC CURATION MAKER - HTML Scraping")
    print(f"URL: {url}")
    print(f"{'='*60}\n")

    # Step 1: Get all product URLs from API
    search_text = extract_search_text(url)
    print(f"Search query: {search_text}\n")

    print("="*60)
    print("STEP 1: Getting all products")
    print("="*60)
    
    all_products = []
    page = 0
    
    # Still use regular requests for API calls (they're fast)
    import requests
    headers = {
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36',
        'Accept': '*/*',
        'Accept-Language': 'en-US,en;q=0.9',
    }
    
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
            time.sleep(0.3)
            
        except Exception as e:
            print(f"❌ Error: {e}")
            break
    
    print(f"\n✓ Total products found: {len(all_products)}")
    
    # Step 2: Scrape all product pages ASYNC (THE FAST PART!)
    print(f"\n{'='*60}")
    print(f"STEP 2: Scraping product pages (ASYNC)")
    print(f"{'='*60}\n")
    print(f"⚡ Scraping {len(all_products)} products with 15 concurrent connections...")
    print(f"This will be ~5x faster than regular scraping!\n")
    
    products_with_text = await scrape_products_batch(all_products, debug)
    
    return products_with_text


async def scrape_products_batch(products: List[Dict], debug: bool = False):
    """
    Scrape multiple products concurrently using async
    """
    connector = aiohttp.TCPConnector(limit=5, force_close=False, ttl_dns_cache=300)  # 10 concurrent connections
    timeout = aiohttp.ClientTimeout(total=60, connect=30)
    
    async with aiohttp.ClientSession(connector=connector, timeout=timeout) as session:
        tasks = []
        for i, product in enumerate(products, 1):
            task = scrape_single_product(session, product, i, len(products), debug and i <= 3)
            tasks.append(task)
        
        # Process all products concurrently
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        # Filter out None results (failed scrapes)
        products_with_text = [r for r in results if r is not None and not isinstance(r, Exception)]
        
        return products_with_text


async def scrape_single_product(session: aiohttp.ClientSession, product: Dict, 
                                index: int, total: int, show_debug: bool = False):
    """
    Scrape a single product page asynchronously
    """
    mp_code = product['mp_code']
    product_url = product['url']
    
    headers = {
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
        'Accept-Language': 'en-US,en;q=0.9',
        'Referer': 'https://luxury.tatacliq.com/'
    }

    await asyncio.sleep(0.3)  # 300ms delay

    # Retry logic - try up to 3 times
    for attempt in range(3):
        try:
            await asyncio.sleep(0.2 * attempt)  # Increasing backoff: 0s, 0.2s, 0.4s

            async with session.get(product_url, headers=headers) as response:
                if response.status != 200:
                    if attempt < 2:  # Try again
                        continue
                    print(f"❌ {index}/{total}: {mp_code} - Failed (status {response.status})")
                    return None

                html = await response.text()

                # Parse HTML
                soup = BeautifulSoup(html, 'html.parser')

                searchable_parts = []

                # Extract content
                searchable_parts.append(product['title'])
                searchable_parts.append(product['brand'])

                # Meta tags
                meta_desc = soup.find('meta', {'name': 'description'})
                if meta_desc and meta_desc.get('content'):
                    searchable_parts.append(meta_desc['content'])

                meta_keywords = soup.find('meta', {'name': 'keywords'})
                if meta_keywords and meta_keywords.get('content'):
                    searchable_parts.append(meta_keywords['content'])

                # Product sections only (exclude recommendations)
                product_sections = soup.find_all(['h1', 'h2', 'h3', 'p', 'div'],
                                                 class_=re.compile(r'product|description|detail|feature|special', re.IGNORECASE))

                for section in product_sections:
                    section_str = str(section.get('class', [])) + str(section.get('id', ''))
                    if any(word in section_str.lower() for word in ['similar', 'viewed', 'recommend', 'related', 'carousel']):
                        continue

                    text = section.get_text(separator=' ', strip=True)
                    searchable_parts.append(text)

                searchable_text = ' '.join(str(p) for p in searchable_parts if p).lower()

                # Progress indicator (every 100 products)
                if index % 100 == 0:
                    print(f"✓ Progress: {index}/{total} products scraped ({index/total*100:.1f}%)")

                if show_debug:
                    print(f"\n{'='*60}")
                    print(f"DEBUG Product {index}")
                    print(f"MP Code: {mp_code}")
                    print(f"URL: {product_url}")
                    print(f"Searchable text length: {len(searchable_text)} chars")
                    print(f"First 500 chars: {searchable_text[:500]}")
                    print(f"{'='*60}\n")

                return {
                    'mp_code': mp_code,
                    'searchable_text': searchable_text,
                    'title': product['title'],
                    'brand': product['brand']
                }

        except asyncio.TimeoutError:
            if attempt < 2:
                await asyncio.sleep(1)  # Wait 1 second before retry
                continue
            print(f"❌ {index}/{total}: {mp_code} - Timeout after 3 attempts")
            return None
        except Exception as e:
            if attempt < 2:
                await asyncio.sleep(1)
                continue
            print(f"❌ {index}/{total}: {mp_code} - Error: {str(e)[:50]}")
            return None

    return None  # All retries failed

def filter_by_keywords(products_data: List[Dict], keywords: List[str]) -> List[str]:
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


def save_multiple_curations(results: Dict[str, List[str]], filename: str = "all_curations.txt"):
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


async def main():
    """
    ASYNC HTML SCRAPING CURATION MAKER
    
    10-20x faster than regular version!
    """
    
    print("\n" + "="*60)
    print("ASYNC CURATION MAKER - 15x FASTER!")
    print("="*60)
    print("\nThis tool will:")
    print("  1. Get all products from API")
    print("  2. Scrape ALL products at once (15 concurrent)")
    print("  3. Filter by multiple keyword groups")
    print("  4. Save all curations to one file")
    print("\n⚡ 15x faster than regular scraping!")
    print("  Example: 6,620 products in ~4 minutes instead of 55 minutes")
    print("="*60 + "\n")

    # Get URL
    url = input("Enter Tata CLiQ Luxury URL: ").strip()
    
    if not url:
        print("Using default URL...")
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

    debug = input("\nShow debug for first 3 products? (y/n): ").strip().lower() == 'y'

    print(f"\n📋 You have {len(curations)} curations:")
    for c in curations:
        print(f"  • {c['name']}: {', '.join(c['keywords'])}")

    input("\nPress Enter to start scraping...")

    # Scrape all products ONCE with async
    start_time = time.time()
    all_products_data = await scrape_all_products_async(url, debug)
    scrape_time = time.time() - start_time

    if not all_products_data:
        print("\n❌ No products scraped!")
        return

    print(f"\n✓ Scraped {len(all_products_data)} products in {scrape_time/60:.1f} minutes")
    print(f"  ({scrape_time/len(all_products_data):.2f} seconds per product)")

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


if __name__ == "__main__":
    asyncio.run(main())
