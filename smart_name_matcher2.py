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
    name_threshold: float = 0.75
    last_name_weight: float = 0.40
    first_name_weight: float = 0.20
    org_weight: float = 0.20
    post_weight: float = 0.15
    mobile_weight: float = 0.05
    stop_first_names: list = None
    stop_penalty: float = 0.75  # Added: Configurable penalty for stop first names
    use_bank_bonus: bool = True  # Added: Option to enable/disable bank bonus
    org_threshold_for_post: float = 0.60  # Added: Threshold for organization similarity to calculate post (default 60% instead of 70%)
    use_shared_lastname_bonus: bool = True  # Added: Bonus when last names share common parts
    shared_lastname_bonus: float = 0.05  # Added: Bonus value for shared last name parts

logging.basicConfig(level=logging.INFO, format="%(levelname)s - %(message)s")


class SmartNameProcessor:
    def __init__(self, settings: Settings = Settings()):
        self.df = None
        self.normalizer = Normalizer()
        self.settings = settings
        self.input_file_format = None

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
        # self.df["FirstName"] = self.correct_text_vectorized(self.df["FirstName"])
        # self.df["LastName"] = self.correct_text_vectorized(self.df["LastName"])
        
        text_cols = ["FirstName", "LastName", "OrganizationTitle", "BankTitle", "Post", "OrganizationTypeTitle", "CompanyTitle", "HoldingTitle"]
        for col in text_cols:
            if col in self.df.columns:
                self.df[col] = self.correct_text_vectorized(self.df[col])

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

    def smart_score(self, f1, l1, f2, l2, org1="", org2="", bank1="", bank2="", post1="", post2="", mobile1="", mobile2="", stop_first_names_set=None):
        # Use max of token_sort_ratio and partial_ratio for names to handle cases where
        # one name is a subset of another (e.g., "پریسا ساعدی" vs "پریسا ساعدی خسروشاهی")
        first_token = fuzz.token_sort_ratio(f1, f2) / 100
        first_partial = fuzz.partial_ratio(f1, f2) / 100
        first_sim = max(first_token, first_partial)
        
        last_token = fuzz.token_sort_ratio(l1, l2) / 100
        last_partial = fuzz.partial_ratio(l1, l2) / 100
        last_sim = max(last_token, last_partial)
        
        if stop_first_names_set and (f1 in stop_first_names_set or f2 in stop_first_names_set):
            first_sim *= self.settings.stop_penalty  # Configurable penalty
        
        # Use max of token_sort_ratio and partial_ratio for organization to better handle
        # cases where one organization name is a subset of another
        # BUT: Avoid false positives when one string is a short substring of another
        # Only use partial_ratio if the shorter string is a significant portion (>=50%) of the longer one
        # (e.g., "مرکزی" is only 13.5% of "پتروشیمی شازند اراک -دفتر مرکزی تهران" - not significant)
        org_token = fuzz.token_sort_ratio(org1, org2) / 100 if org1 and org2 else 0
        org_partial = fuzz.partial_ratio(org1, org2) / 100 if org1 and org2 else 0
        
        if org1 and org2:
            len_ratio = min(len(org1), len(org2)) / max(len(org1), len(org2))
            # If shorter string is less than 50% of longer string AND partial is much higher than token,
            # it's probably just a small substring match, not real similarity - use token_sort_ratio
            # This prevents cases like "مرکزی" matching "پتروشیمی شازند اراک -دفتر مرکزی تهران"
            if len_ratio < 0.5 and org_partial > org_token + 0.3:
                org_sim = org_token
            else:
                # If shorter string is >= 50% of longer, it's a significant portion - use partial_ratio
                org_sim = max(org_token, org_partial)
        else:
            org_sim = max(org_token, org_partial)
        
        # Only calculate post similarity if organization similarity meets threshold (default 60% instead of 70%)
        if org_sim >= self.settings.org_threshold_for_post and post1 and post2:
            # Apply same logic for post: only use partial_ratio if shorter string is >=50% of longer one
            post_token = fuzz.token_sort_ratio(post1, post2) / 100
            post_partial = fuzz.partial_ratio(post1, post2) / 100
            len_ratio_post = min(len(post1), len(post2)) / max(len(post1), len(post2)) if post1 and post2 else 1
            # If shorter string is less than 50% of longer string AND partial is much higher than token,
            # it's probably just a small substring match - use token_sort_ratio
            if len_ratio_post < 0.5 and post_partial > post_token + 0.3:
                post_sim = post_token
            else:
                # If shorter string is >= 50% of longer, it's a significant portion - use partial_ratio
                post_sim = max(post_token, post_partial)
        else:
            post_sim = 0
        
        mobile_sim = 0
        if mobile1 and mobile2:
            mobile1_clean = ''.join(filter(str.isdigit, str(mobile1)))[-11:]
            mobile2_clean = ''.join(filter(str.isdigit, str(mobile2)))[-11:]
            
            if len(mobile1_clean) >= 10 and len(mobile2_clean) >= 10:
                similarity = fuzz.ratio(mobile1_clean, mobile2_clean) / 100.0
                
                if similarity >= 0.80:
                    mobile_sim = (similarity - 0.80) * 0.5
                else:
                    mobile_sim = 0 
        
        bank_bonus = 0
        last_weight = self.settings.last_name_weight
        if self.settings.use_bank_bonus and bank1 and bank2:  # Check for use_bank_bonus option
            bank_sim = fuzz.ratio(bank1, bank2) / 100
            if bank_sim >= 0.8:
                bank_bonus = 0.05
                last_weight -= 0.05
        
        # Bonus for shared last name parts (e.g., "نجفی" in "نجفی مطیعی")
        shared_lastname_bonus = 0
        if self.settings.use_shared_lastname_bonus and l1 and l2:
            # Check if one last name contains the other (after normalization)
            l1_words = set(l1.split())
            l2_words = set(l2.split())
            # If there are common words or one is subset of another
            if l1_words.intersection(l2_words) or (l1 in l2 or l2 in l1):
                # Only give bonus if partial_ratio is high (meaning significant overlap)
                if last_partial >= 0.8:
                    shared_lastname_bonus = self.settings.shared_lastname_bonus
        
        score = (
            last_weight * last_sim +
            self.settings.first_name_weight * first_sim +
            self.settings.org_weight * org_sim +
            self.settings.post_weight * post_sim +
            self.settings.mobile_weight * mobile_sim +
            bank_bonus +
            shared_lastname_bonus
        )
        return round(score, 3)

    def extract_stop_first_names(self, min_frequency=5):
        first_names = self.df["FirstName"].astype(str).str.strip()
        first_names = first_names[first_names != ""]
        if len(first_names) == 0:
            self.settings.stop_first_names = []
            return
        freq = first_names.value_counts()
        self.settings.stop_first_names = list(freq[freq >= min_frequency].index)
        logging.info(f"Extracted stop first names: {self.settings.stop_first_names}")

    def find_similar_names(self, output_path="final_smart_similar_names.xlsx"):
        # Pre-extract all data once to avoid repeated df.loc calls
        records = []
        for idx, row in self.df.iterrows():
            first_name = str(row["FirstName"]).strip()
            last_name = str(row["LastName"]).strip()
            if first_name or last_name:  # Keep even if one is empty
                # Pre-extract all columns we'll need
                org = str(row.get("OrganizationTitle", ""))
                bank = str(row.get("BankTitle", ""))
                post = str(row.get("Post", ""))
                phone = str(row.get("MobileNumber", ""))
                org_type = str(row.get("OrganizationTypeTitle", ""))
                company = str(row.get("CompanyTitle", ""))
                holding = str(row.get("HoldingTitle", ""))
                is_head = row.get("IsHead", None)
                
                records.append((idx, first_name, last_name, org, bank, post, phone, 
                              org_type, company, holding, is_head))
        
        # Convert stop_first_names to set for O(1) lookup
        stop_first_names_set = set(self.settings.stop_first_names) if self.settings.stop_first_names else set()
        
        results = []
        seen_pairs = set()
        
        logging.info(f"Dataset size: {len(records)}. Using optimized last_name-based pre-filter.")
        
        for i in range(len(records)):
            idx1, f1, l1, org1, bank1, post1, phone1, org_type1, company1, holding1, is_head1 = records[i]
            name1_full = f"{f1} {l1}".strip()
            
            # Only process records ahead to avoid duplicates
            other_records = records[i+1:]
            
            if not other_records:
                continue
            
            # Optimized pre-filter: use faster partial_ratio directly instead of process.extract
            # This avoids the overhead of process.extract for each record
            candidate_indices = []
            for j, (idx2, f2, l2, _, _, _, _, _, _, _, _) in enumerate(other_records):
                # Quick last name similarity check (faster than process.extract)
                last_sim_score = fuzz.partial_ratio(l1, l2)
                if last_sim_score >= 50:  # Only if last name similarity >= 50%
                    candidate_indices.append((j, idx2, f2, l2, last_sim_score))
            
            # Limit to top candidates by last name similarity to avoid too many smart_score calls
            if len(candidate_indices) > 100:
                # Sort by last name similarity (already computed) and take top 100
                candidate_indices.sort(key=lambda x: x[4], reverse=True)  # x[4] is last_sim_score
                candidate_indices = candidate_indices[:100]
            
            # Remove the similarity score from tuples before processing
            candidate_indices = [(j, idx2, f2, l2) for j, idx2, f2, l2, _ in candidate_indices]
            
            for j, idx2, f2, l2 in candidate_indices:
                # Get pre-extracted data for record 2
                _, _, _, org2, bank2, post2, phone2, org_type2, company2, holding2, is_head2 = other_records[j]
                name2_full = f"{f2} {l2}".strip()
                
                final_score = self.smart_score(f1, l1, f2, l2, org1, org2, bank1, bank2, post1, post2, phone1, phone2, stop_first_names_set)
                
                # Check if names are exactly the same (after normalization) - include regardless of threshold
                exact_name_match = (f1 == f2 and l1 == l2)
                
                # If names are exactly the same, set score to 0.8 (80%) to ensure they appear at the top of the list
                if exact_name_match:
                    final_score = 0.8
                
                if exact_name_match or final_score >= self.settings.name_threshold:
                    pair_key = tuple(sorted([name1_full, name2_full]))
                    if pair_key not in seen_pairs:
                        seen_pairs.add(pair_key)
                        
                        # Modify organization title for output: if IsHead is False (0), append BankTitle with dash
                        org1_output = org1
                        org2_output = org2
                        
                        if is_head1 is not None and (is_head1 == 0 or is_head1 == False):
                            if bank1 and bank1.strip():
                                org1_output = f"{org1} - {bank1}".strip() if org1 else bank1
                        
                        if is_head2 is not None and (is_head2 == 0 or is_head2 == False):
                            if bank2 and bank2.strip():
                                org2_output = f"{org2} - {bank2}".strip() if org2 else bank2
                        
                        results.append([
                            name1_full, post1, org1_output, org_type1, company1, holding1, phone1,
                            name2_full, post2, org2_output, org_type2, company2, holding2, phone2,
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
                    "and saves results. Example usage: python script.py input.xlsx --name_threshold 0.85 -- 2 --use_bank_bonus False"
    )
    parser.add_argument("input_file", type=str, help="Path to input file (CSV or Excel). Must contain 'FirstName' and 'LastName' columns.")
    parser.add_argument("--output_similar", type=str, default="final_smart_similar_names.xlsx", help="Output path for similar names file (CSV or Excel).")
    parser.add_argument("--name_threshold", type=float, default=0.75, help="Similarity threshold for considering names similar (0.0-1.0).")
    parser.add_argument("--last_weight", type=float, default=0.40, help="Weight for last name in scoring.")
    parser.add_argument("--first_weight", type=float, default=0.20, help="Weight for first name in scoring.")
    parser.add_argument("--org_weight", type=float, default=0.20, help="Weight for organization in scoring.")
    parser.add_argument("--post_weight", type=float, default=0.15, help="Weight for post in scoring.")
    parser.add_argument("--mobile_weight", type=float, default=0.05, help="Weight for mobile number in scoring.")
    parser.add_argument("--min_freq", type=int, default=5, help="Minimum frequency for extracting stop first names.")
    parser.add_argument("--stop_penalty", type=float, default=0.8, help="Penalty multiplier for common first names (0.0-1.0).")
    parser.add_argument("--use_bank_bonus", type=bool, default=True, help="Whether to use bank bonus in scoring (True/False).")
    parser.add_argument("--org_threshold_for_post", type=float, default=0.60, help="Organization similarity threshold to calculate post similarity (0.0-1.0, default 0.60).")
    parser.add_argument("--use_shared_lastname_bonus", type=bool, default=True, help="Whether to use bonus for shared last name parts (True/False).")
    parser.add_argument("--shared_lastname_bonus", type=float, default=0.05, help="Bonus value for shared last name parts (0.0-1.0, default 0.05).")

    args = parser.parse_args()

    settings = Settings(
            name_threshold=args.name_threshold,
            last_name_weight=args.last_weight,
            first_name_weight=args.first_weight,
            org_weight=args.org_weight,
            post_weight=args.post_weight,
            mobile_weight=args.mobile_weight,
            stop_penalty=args.stop_penalty,
            use_bank_bonus=args.use_bank_bonus,
            org_threshold_for_post=args.org_threshold_for_post,
            use_shared_lastname_bonus=args.use_shared_lastname_bonus,
            shared_lastname_bonus=args.shared_lastname_bonus
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