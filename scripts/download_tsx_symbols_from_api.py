"""
Download ALL TSX/TSXV symbols from the official TMX JSON API.

This script fetches the complete list of trading symbols directly from the TMX Group API,
which includes ALL variants (Class A/B/X/Y, preferred shares, debentures, warrants, etc.).

Output: data/tsx_all_symbols.csv with columns: symbol, name, parent_symbol
"""

import requests
import json
import pandas as pd
from pathlib import Path
from utils.logger import get_logger

logger = get_logger(__name__)

def download_tsx_symbols():
    """Download all TSX symbols from the official TMX API."""
    
    # The TSX company directory JSON endpoint
    url = "https://www.tsx.com/json/company-directory/search/tsx/%5E*"
    
    logger.info(f"Downloading TSX symbols from {url}")
    
    try:
        response = requests.get(url, timeout=30)
        response.raise_for_status()
        
        data = response.json()
        logger.info(f"Successfully downloaded JSON data")
        
        # Parse the JSON structure
        # Each entry has: symbol, name, instruments[{symbol, name}]
        all_symbols = []
        
        if 'results' in data:
            for company in data['results']:
                parent_symbol = company.get('symbol', '')
                parent_name = company.get('name', '')
                
                # Each company has an 'instruments' array with actual trading symbols
                instruments = company.get('instruments', [])
                
                for instrument in instruments:
                    symbol = instrument.get('symbol', '')
                    name = instrument.get('name', '')
                    
                    if symbol:
                        all_symbols.append({
                            'symbol': symbol,
                            'name': name,
                            'parent_symbol': parent_symbol,
                            'parent_name': parent_name
                        })
        
        logger.info(f"Parsed {len(all_symbols)} trading symbols")
        
        # Convert to DataFrame
        df = pd.DataFrame(all_symbols)
        
        # Save to CSV
        output_path = Path('data/tsx_all_symbols.csv')
        output_path.parent.mkdir(exist_ok=True)
        df.to_csv(output_path, index=False)
        
        logger.info(f"Saved {len(df)} symbols to {output_path}")
        
        # Show some examples
        logger.info("\n=== Examples of multi-class stocks ===")
        
        # Find symbols with variants
        parent_counts = df.groupby('parent_symbol').size()
        multi_class = parent_counts[parent_counts > 1].head(10)
        
        for parent_symbol in multi_class.index:
            variants = df[df['parent_symbol'] == parent_symbol]
            logger.info(f"\n{parent_symbol} ({variants.iloc[0]['parent_name']}):")
            for _, row in variants.iterrows():
                logger.info(f"  {row['symbol']}: {row['name']}")
        
        # Count suffix types
        logger.info("\n=== Suffix Statistics ===")
        df['suffix'] = df['symbol'].str.extract(r'\.([A-Z]+)$')
        suffix_counts = df['suffix'].value_counts()
        logger.info(f"\nTop suffixes:\n{suffix_counts.head(20)}")
        
        return df
        
    except requests.RequestException as e:
        logger.error(f"Failed to download TSX symbols: {e}")
        raise
    except json.JSONDecodeError as e:
        logger.error(f"Failed to parse JSON response: {e}")
        raise

def download_tsxv_symbols():
    """Download all TSX Venture symbols."""
    
    url = "https://www.tsx.com/json/company-directory/search/tsxv/%5E*"
    
    logger.info(f"Downloading TSXV symbols from {url}")
    
    try:
        response = requests.get(url, timeout=30)
        response.raise_for_status()
        
        data = response.json()
        logger.info(f"Successfully downloaded TSXV JSON data")
        
        all_symbols = []
        
        if 'results' in data:
            for company in data['results']:
                parent_symbol = company.get('symbol', '')
                parent_name = company.get('name', '')
                
                instruments = company.get('instruments', [])
                
                for instrument in instruments:
                    symbol = instrument.get('symbol', '')
                    name = instrument.get('name', '')
                    
                    if symbol:
                        all_symbols.append({
                            'symbol': symbol,
                            'name': name,
                            'parent_symbol': parent_symbol,
                            'parent_name': parent_name,
                            'exchange': 'TSXV'
                        })
        
        logger.info(f"Parsed {len(all_symbols)} TSXV trading symbols")
        
        return pd.DataFrame(all_symbols)
        
    except requests.RequestException as e:
        logger.error(f"Failed to download TSXV symbols: {e}")
        return pd.DataFrame()

if __name__ == '__main__':
    logger.info("Starting TSX/TSXV symbol download from official API")
    
    # Download TSX
    tsx_df = download_tsx_symbols()
    tsx_df['exchange'] = 'TSX'
    
    # Download TSXV
    tsxv_df = download_tsxv_symbols()
    
    # Combine
    if not tsxv_df.empty:
        all_df = pd.concat([tsx_df, tsxv_df], ignore_index=True)
        
        output_path = Path('data/tsx_tsxv_all_symbols.csv')
        all_df.to_csv(output_path, index=False)
        
        logger.info(f"\n=== FINAL SUMMARY ===")
        logger.info(f"Total symbols: {len(all_df)}")
        logger.info(f"TSX symbols: {len(tsx_df)}")
        logger.info(f"TSXV symbols: {len(tsxv_df)}")
        logger.info(f"Saved combined list to {output_path}")
    
    logger.info("Download complete!")
