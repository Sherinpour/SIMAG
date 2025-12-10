import pandas as pd
import re
import logging
import os
from dataclasses import dataclass
from hazm import Normalizer
from rapidfuzz import fuzz, process
import argparse
import time  # Added for execution time logging

# Settings
@dataclass
class Settings:
    name_threshold: float = 0.80
    last_name_weight: float = 0.45
    first_name_weight: float = 0.2
    org_weight: float = 0.25
    post_weight: float = 0.05
    mobile_weight: float = 0.1
    stop_first_names: list = None
    stop_penalty: float = 0.8  # Added: Configurable penalty for stop first names
    use_bank_bonus: bool = True  # Added: Option to enable/disable bank bonus

logging.basicConfig(level=logging.INFO, format="%(levelname)s - %(message)s")

PREFIX_PATTERN = re.compile(r"""
^(جناب\s*آقای|سرکار\s*خانم|آقای|خانم|دکتر|مهندس|استاد|حاج|حاجی|
کربلایی|جناب|سرکار|پروفسور|سید|سیده|بانو|میر|ملا|حجت‌الاسلام|آیت‌الله|
خان|مشهدی)\s* 
""", re.VERBOSE | re.IGNORECASE)

class SmartNameProcessor:
    def __init__(self, settings: Settings = Settings()):
        self.df = None
        self.normalizer = Normalizer()
        self.settings = settings
        self.input_file_format = None

    def remove_prefix_vectorized(self, series):
        return series.str.replace(PREFIX_PATTERN, "", regex=True).str.strip()

    def correct_text_vectorized(self, series):
        return series.apply(self.normalizer.normalize)  # Still using apply as hazm is not vectorized, but fast enough for 3000 records

    def load_excel(self, file_path):
        file_ext = os.path.splitext(file_path)[1].lower()
        if file_ext == '.csv':
            encodings = ['utf-8-sig', 'utf-8', 'cp1256', 'latin-1']
            for enc in encodings:
                try:
                    self.df = pd.read_csv(file_path, encoding=enc)
                    break
                except UnicodeDecodeError:
                    continue
            else:
                raise ValueError("Could not decode CSV file.")
            self.input_file_format = 'csv'
        elif file_ext in ['.xlsx', '.xls']:
            self.df = pd.read_excel(file_path)
            self.input_file_format = 'excel'
        else:
            raise ValueError("Unsupported file format.")
        
        required_cols = {"FirstName", "LastName"}
        if not required_cols.issubset(self.df.columns):
            raise ValueError("Missing required columns: FirstName and LastName.")
        
        # Check optional fields and log warning if they don't exist
        optional_cols = {
            "OrganizationTitle", "BankTitle", "Post", "MobileNumber",
            "OrganizationTypeTitle", "CompanyTitle", "HoldingTitle"
        }
        missing_optional = optional_cols - set(self.df.columns)
        if missing_optional:
            logging.warning(f"Missing optional columns: {', '.join(missing_optional)}. Using empty strings as default.")
        
        # Fill NaN with empty string for safety
        self.df.fillna("", inplace=True)
        return self.df

    def process_names(self):
        self.df["FirstName"] = self.remove_prefix_vectorized(self.df["FirstName"].astype(str))
        self.df["FirstName"] = self.correct_text_vectorized(self.df["FirstName"])
        
        self.df["LastName"] = self.remove_prefix_vectorized(self.df["LastName"].astype(str))
        self.df["LastName"] = self.correct_text_vectorized(self.df["LastName"])
        
        logging.info("Names processed (vectorized where possible).")

    def save(self, output_path):
        output_ext = os.path.splitext(output_path)[1].lower()
        if not output_ext:
            output_ext = '.csv' if self.input_file_format == 'csv' else '.xlsx'
            output_path += output_ext
        
        if output_ext == '.csv':
            self.df.to_csv(output_path, index=False, encoding='utf-8-sig')
        else:
            self.df.to_excel(output_path, index=False)
        logging.info(f"Saved to {output_path}")

    def smart_score(self, f1, l1, f2, l2, org1="", org2="", bank1="", bank2="", post1="", post2="", mobile1="", mobile2=""):
        first_sim = fuzz.token_sort_ratio(f1, f2) / 100  # Improvement: token_sort for word order
        last_sim = fuzz.token_sort_ratio(l1, l2) / 100
        
        if self.settings.stop_first_names and (f1 in self.settings.stop_first_names or f2 in self.settings.stop_first_names):
            first_sim *= self.settings.stop_penalty  # Configurable penalty
        
        org_sim = fuzz.partial_ratio(org1, org2) / 100 if org1 and org2 else 0
        post_sim = fuzz.partial_ratio(post1, post2) / 100 if post1 and post2 else 0
        
        mobile_sim = 0
        if mobile1 and mobile2:
            mobile1_clean = ''.join(filter(str.isdigit, str(mobile1)))[-11:]  # Normalize to last 11 digits
            mobile2_clean = ''.join(filter(str.isdigit, str(mobile2)))[-11:]
            if mobile1_clean and mobile2_clean:
                if mobile1_clean == mobile2_clean:
                    mobile_sim = 1.0
                else:
                    mobile_sim = fuzz.ratio(mobile1_clean, mobile2_clean) / 100
        
        bank_bonus = 0
        last_weight = self.settings.last_name_weight
        if self.settings.use_bank_bonus and bank1 and bank2:  # Check for use_bank_bonus option
            bank_sim = fuzz.ratio(bank1, bank2) / 100
            if bank_sim >= 0.8:
                bank_bonus = 0.1
                last_weight -= 0.1
        
        score = (
            last_weight * last_sim +
            self.settings.first_name_weight * first_sim +
            self.settings.org_weight * org_sim +
            self.settings.post_weight * post_sim +
            self.settings.mobile_weight * mobile_sim +
            bank_bonus
        )
        return round(score, 3)

    def extract_stop_first_names(self, min_frequency=3):
        first_names = self.df["FirstName"].astype(str).str.strip()
        first_names = first_names[first_names != ""]
        if len(first_names) == 0:
            self.settings.stop_first_names = []
            return
        freq = first_names.value_counts()
        self.settings.stop_first_names = list(freq[freq >= min_frequency].index)
        logging.info(f"Extracted stop first names: {self.settings.stop_first_names}")

    def find_similar_names(self, output_path="final_smart_similar_names.xlsx"):
        records = []
        for idx, row in self.df.iterrows():
            first_name = str(row["FirstName"]).strip()
            last_name = str(row["LastName"]).strip()
            if first_name or last_name:  # Keep even if one is empty
                records.append((idx, first_name, last_name))
        
        results = []
        seen_pairs = set()
        
        # For small datasets (<3000), compare all pairs - fast with rapidfuzz
        logging.info(f"Dataset size: {len(records)}. Comparing all pairs (optimized for small data).")
        for i in range(len(records)):
            idx1, f1, l1 = records[i]
            name1_full = f"{f1} {l1}".strip()
            
            other_records = [(idx, f, l) for idx, f, l in records[i+1:]]  # Only ahead to avoid duplicates
            other_names = [f"{f} {l}".strip() for _, f, l in other_records]
            
            if not other_names:
                continue
            
            matches = process.extract(name1_full, other_names, scorer=fuzz.token_sort_ratio, limit=20)
            
            for match_name, score, j in matches:
                idx2, f2, l2 = other_records[j]
                org1 = str(self.df.loc[idx1].get("OrganizationTitle", ""))
                org2 = str(self.df.loc[idx2].get("OrganizationTitle", ""))
                bank1 = str(self.df.loc[idx1].get("BankTitle", ""))
                bank2 = str(self.df.loc[idx2].get("BankTitle", ""))
                post1 = str(self.df.loc[idx1].get("Post", ""))
                post2 = str(self.df.loc[idx2].get("Post", ""))
                phone1 = str(self.df.loc[idx1].get("MobileNumber", ""))
                phone2 = str(self.df.loc[idx2].get("MobileNumber", ""))
                
                final_score = self.smart_score(f1, l1, f2, l2, org1, org2, bank1, bank2, post1, post2, phone1, phone2)
                
                if final_score >= self.settings.name_threshold:
                    pair_key = tuple(sorted([f"{f1} {l1}", f"{f2} {l2}"]))
                    if pair_key not in seen_pairs:
                        seen_pairs.add(pair_key)
                        org_type1 = str(self.df.loc[idx1].get("OrganizationTypeTitle", ""))
                        company1 = str(self.df.loc[idx1].get("CompanyTitle", ""))
                        holding1 = str(self.df.loc[idx1].get("HoldingTitle", ""))
                        org_type2 = str(self.df.loc[idx2].get("OrganizationTypeTitle", ""))
                        company2 = str(self.df.loc[idx2].get("CompanyTitle", ""))
                        holding2 = str(self.df.loc[idx2].get("HoldingTitle", ""))
                        
                        results.append([
                            f"{f1} {l1}".strip(), post1, org1, org_type1, company1, holding1, phone1,
                            f"{f2} {l2}".strip(), post2, org2, org_type2, company2, holding2, phone2,
                            final_score * 100  # Convert to percentage for readability
                        ])
        
        df_result = pd.DataFrame(results, columns=[
            "نام اول", "پست اول", "سازمان اول", "نوع سازمان اول", "عنوان شرکت اول", "عنوان هولدینگ اول", "شماره تلفن اول",
            "نام دوم", "پست دوم", "سازمان دوم", "نوع سازمان دوم", "عنوان شرکت دوم", "عنوان هولدینگ دوم", "شماره تلفن دوم",
            "درصد تشابه"
        ])
        df_result = df_result.sort_values("درصد تشابه", ascending=False)
        
        if output_path:
            output_ext = os.path.splitext(output_path)[1].lower()
            if output_ext == '.csv':
                df_result.to_csv(output_path, index=False, encoding='utf-8-sig')
            else:
                df_result.to_excel(output_path, index=False)
            logging.info(f"Results saved to {output_path}")
        
        return df_result

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Optimized Smart Name Processor for small datasets (up to 3000 records). "
                    "Processes names, extracts common first names, finds similar names based on fuzzy matching, "
                    "and saves results. Example usage: python script.py input.xlsx --name_threshold 0.85 --min_freq 2 --use_bank_bonus False"
    )
    parser.add_argument("input_file", type=str, help="Path to input file (CSV or Excel). Must contain 'FirstName' and 'LastName' columns.")
    parser.add_argument("--output_similar", type=str, default="final_smart_similar_names.xlsx", help="Output path for similar names file (CSV or Excel).")
    parser.add_argument("--name_threshold", type=float, default=0.80, help="Similarity threshold for considering names similar (0.0-1.0).")
    parser.add_argument("--last_weight", type=float, default=0.45, help="Weight for last name in scoring.")
    parser.add_argument("--first_weight", type=float, default=0.2, help="Weight for first name in scoring.")
    parser.add_argument("--org_weight", type=float, default=0.25, help="Weight for organization in scoring.")
    parser.add_argument("--post_weight", type=float, default=0.05, help="Weight for post in scoring.")
    parser.add_argument("--mobile_weight", type=float, default=0.1, help="Weight for mobile number in scoring.")
    parser.add_argument("--min_freq", type=int, default=3, help="Minimum frequency for extracting stop first names.")
    parser.add_argument("--stop_penalty", type=float, default=0.8, help="Penalty multiplier for common first names (0.0-1.0).")
    parser.add_argument("--use_bank_bonus", type=bool, default=True, help="Whether to use bank bonus in scoring (True/False).")

    args = parser.parse_args()

    settings = Settings(
            name_threshold=args.name_threshold,
            last_name_weight=args.last_weight,
            first_name_weight=args.first_weight,
            org_weight=args.org_weight,
            post_weight=args.post_weight,
            mobile_weight=args.mobile_weight,
            stop_penalty=args.stop_penalty,  # Added
            use_bank_bonus=args.use_bank_bonus  # Added
    )

    start_time = time.time()  # Start timing

    processor = SmartNameProcessor(settings)
    processor.load_excel(args.input_file)
    processor.process_names()
    processor.extract_stop_first_names(args.min_freq)
    processor.save(args.input_file)
    processor.find_similar_names(args.output_similar)

    execution_time = time.time() - start_time
    logging.info(f"Total execution time: {execution_time:.2f} seconds")