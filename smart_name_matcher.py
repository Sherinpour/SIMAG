import pandas as pd
import re
import logging
import os
from dataclasses import dataclass
from hazm import Normalizer
from rapidfuzz import fuzz, process
from tqdm import tqdm
import argparse

# ✅ ---------------- SETTINGS ----------------
@dataclass
class Settings:
    """
    Configuration settings for the SmartNameProcessor.
    
    :param name_threshold: Threshold for name similarity score (default: 0.80)
    :param org_threshold: Threshold for organization similarity (default: 0.5, not currently used)
    :param position_threshold: Threshold for position similarity (default: 0.6, not currently used)
    :param last_name_weight: Weight for last name in scoring (default: 0.5)
    :param first_name_weight: Weight for first name in scoring (default: 0.2)
    :param org_weight: Weight for organization in scoring (default: 0.3)
    :param stop_first_names: List of common first names to penalize (default: None)
    """
    name_threshold: float = 0.80
    org_threshold: float = 0.5
    position_threshold: float = 0.6
    last_name_weight: float = 0.5
    first_name_weight: float = 0.2
    org_weight: float = 0.3
    stop_first_names: list = None

# ✅ ---------------- LOGGING ----------------
logging.basicConfig(level=logging.INFO, format="%(levelname)s - %(message)s")

# ✅ ---------------- PREFIX REGEX ----------------
PREFIX_PATTERN = re.compile(r"""
^(جناب\s*آقای|سرکار\s*خانم|آقای|خانم|دکتر|مهندس|استاد|حاج|حاجی|
کربلایی|جناب|سرکار|پروفسور|سید|سیده|بانو|میر|ملا|حجت‌الاسلام|آیت‌الله|
خان|مشهدی)\s*
""", re.VERBOSE)

# ✅ ---------------- CLASS ----------------
class SmartNameProcessor:
    """
    A class for processing and matching Persian names from Excel or CSV files.
    
    This class loads an Excel or CSV file, cleans and normalizes names, extracts common first names,
    and finds similar names using fuzzy matching with blocking for efficiency.
    """

    def __init__(self, settings: Settings = Settings()):
        """
        Initialize the SmartNameProcessor.
        
        :param settings: Optional Settings object to customize thresholds and weights.
        """
        self.df = None
        self.name_column = None
        self.normalizer = Normalizer()
        self.settings = settings
        self.input_file_format = None  # Track input file format

    # ✅ 1. REGEX PREFIX REMOVAL
    def remove_prefix_fast(self, name):
        """
        Remove common Persian prefixes from the name using regex.
        
        :param name: The name string to process.
        :return: Name without prefix.
        """
        if pd.isna(name) or not str(name).strip():
            return name
        name = self.normalizer.normalize(str(name)).strip()
        return re.sub(PREFIX_PATTERN, "", name).strip()

    # ✅ 2. SMART PERSIAN EDITOR
    def correct_text_fast(self, name):
        """
        Normalize Persian text using hazm Normalizer.
        """
        if not name:
            return name
        return self.normalizer.normalize(str(name))

    # ✅ 3. LOAD FILE (EXCEL OR CSV)
    def load_excel(self, file_path):
        """
        Load the Excel or CSV file and check for required columns.
        
        Automatically detects file format based on extension.
        Checks for required columns FirstName and LastName.
        
        :param file_path: Path to the Excel or CSV file.
        :return: Loaded DataFrame.
        """
        logging.info(f"Loading file: {file_path}")
        
        # Detect file format from extension
        file_ext = os.path.splitext(file_path)[1].lower()
        
        if file_ext == '.csv':
            # Read CSV with UTF-8 encoding (common for Persian text)
            try:
                self.df = pd.read_csv(file_path, encoding='utf-8-sig')
            except UnicodeDecodeError:
                # Try other encodings if UTF-8 fails
                try:
                    self.df = pd.read_csv(file_path, encoding='utf-8')
                except:
                    self.df = pd.read_csv(file_path, encoding='latin-1')
            self.input_file_format = 'csv'
            logging.info("Detected CSV format")
        elif file_ext in ['.xlsx', '.xls']:
            self.df = pd.read_excel(file_path)
            self.input_file_format = 'excel'
            logging.info("Detected Excel format")
        else:
            # Default to Excel for backward compatibility
            logging.warning(f"Unknown file extension '{file_ext}', trying Excel format...")
            try:
                self.df = pd.read_excel(file_path)
                self.input_file_format = 'excel'
            except:
                # If Excel fails, try CSV
                try:
                    self.df = pd.read_csv(file_path, encoding='utf-8-sig')
                    self.input_file_format = 'csv'
                except:
                    raise ValueError(f"Could not read file '{file_path}'. Supported formats: .xlsx, .xls, .csv")

        if not {"FirstName", "LastName"}.issubset(self.df.columns):
            raise ValueError(
                "Missing required columns. Both 'FirstName' and 'LastName' must exist."
            )
        
        logging.info("Columns 'FirstName' and 'LastName' found.")

        return self.df

    # ✅ 4. PROCESS NAMES
    def process_names(self):
        """
        Process names by removing prefixes and correcting text.
        FirstName and LastName are processed separately.
        """
        tqdm.pandas()

        try:
            self.df["FirstName"] = self.df["FirstName"].progress_apply(self.remove_prefix_fast)
        except Exception as e:
            logging.error(f"❌ [ERROR] Failed to remove prefixes from FirstName: {e}")
            logging.warning(f"⚠️ [WARNING] Continuing with original FirstName values...")
        
        try:
            self.df["FirstName"] = self.df["FirstName"].progress_apply(self.correct_text_fast)
        except Exception as e:
            logging.error(f"❌ [ERROR] Failed to correct FirstName text: {e}")
            logging.warning(f"⚠️ [WARNING] Continuing with uncorrected FirstName values...")
        
        try:
            self.df["LastName"] = self.df["LastName"].progress_apply(self.remove_prefix_fast)
        except Exception as e:
            logging.error(f"❌ [ERROR] Failed to remove prefixes from LastName: {e}")
            logging.warning(f"⚠️ [WARNING] Continuing with original LastName values...")
        
        try:
            self.df["LastName"] = self.df["LastName"].progress_apply(self.correct_text_fast)
        except Exception as e:
            logging.error(f"❌ [ERROR] Failed to correct LastName text: {e}")
            logging.warning(f"⚠️ [WARNING] Continuing with uncorrected LastName values...")

        logging.info("Names normalized & corrected.")

    # ✅ 5. SAVE
    def save(self, output_path):
        """
        Save the processed DataFrame to Excel or CSV file.
        
        Automatically detects output format from file extension.
        If no extension provided, uses the same format as input file.
        
        :param output_path: Path to save the output file.
        """
        output_ext = os.path.splitext(output_path)[1].lower()
        
        # If no extension, use input file format
        if not output_ext and self.input_file_format:
            if self.input_file_format == 'csv':
                output_path = output_path + '.csv'
                output_ext = '.csv'
            else:
                output_path = output_path + '.xlsx'
                output_ext = '.xlsx'
        
        if output_ext == '.csv':
            self.df.to_csv(output_path, index=False, encoding='utf-8-sig')
            logging.info(f"Saved to {output_path} (CSV format)")
        else:
            self.df.to_excel(output_path, index=False)
            logging.info(f"Saved to {output_path} (Excel format)")

    # ✅ 6. BLOCKING KEY (REMOVES O(n²))
    def blocking_key(self, last_name):
        """
        Generate a blocking key based on the first 3 characters of the last name.
        
        :param last_name: The last name string.
        :return: Blocking key string.
        """
        if pd.isna(last_name) or not str(last_name).strip():
            return ""
        return str(last_name).strip()[:3]

    # ✅ 7. WEIGHTED SMART SIMILARITY
    def smart_score(self, f1, l1, f2, l2, org1="", org2="", bank1="", bank2=""):
        """
        Calculate a weighted similarity score between two names and organizations.
        
        Uses configurable weights from settings.
        
        :param f1: First person's first name.
        :param l1: First person's last name.
        :param f2: Second person's first name.
        :param l2: Second person's last name.
        :param org1: First organization (optional).
        :param org2: Second organization (optional).
        :param bank1: First bank title (optional).
        :param bank2: Second bank title (optional).
        :return: Rounded similarity score.
        """

        first_sim = fuzz.ratio(f1, f2) / 100
        last_sim = fuzz.ratio(l1, l2) / 100

        # ✅ stop names penalty
        if self.settings.stop_first_names:
            if f1 in self.settings.stop_first_names:
                first_sim *= 0.6
            if f2 in self.settings.stop_first_names:
                first_sim *= 0.6

        org_sim = fuzz.ratio(org1, org2) / 100 if org1 and org2 else 0

        # ✅ Bank similarity logic (NEW)
        bank_bonus = 0
        if bank1 and bank2:
            bank_sim = fuzz.ratio(bank1, bank2) / 100
            if bank_sim >= 0.8:
                bank_bonus = 0.1
                last_weight = self.settings.last_name_weight - 0.1
            else:
                last_weight = self.settings.last_name_weight
        else:
            last_weight = self.settings.last_name_weight

        score = (
            last_weight * last_sim +
            self.settings.first_name_weight * first_sim +
            self.settings.org_weight * org_sim +
            bank_bonus
        )

        return round(score, 3)

    # ✅ AUTO STOP FIRST NAMES FROM EXCEL
    def extract_stop_first_names(self, min_frequency=3):
        """
        Automatically extract common first names from the DataFrame.
        
        :param min_frequency: Minimum frequency to consider a first name as 'stop' (default: 3).
        """
        try:
            if self.df is None or "FirstName" not in self.df.columns:
                logging.warning(f"⚠️ [WARNING] Cannot extract stop names: DataFrame or FirstName column not available")
                self.settings.stop_first_names = []
                return
            
            first_names = self.df["FirstName"].dropna().astype(str).str.strip()
            first_names = first_names[first_names != ""]

            if len(first_names) == 0:
                logging.warning(f"⚠️ [WARNING] No first names found to extract stop names")
                self.settings.stop_first_names = []
                return

            freq = first_names.value_counts()

            self.settings.stop_first_names = list(
                freq[freq >= min_frequency].index
            )

            logging.info(
                f"✅ Auto stop_first_names extracted: {self.settings.stop_first_names}"
            )
        except Exception as e:
            logging.error(f"❌ [ERROR] Failed to extract stop first names: {e}")
            logging.warning(f"⚠️ [WARNING] Continuing without stop names filter...")
            self.settings.stop_first_names = []

    # ✅ 8. FAST MATCHING ENGINE
    def find_similar_names(self, output_path="final_smart_similar_names.xlsx"):
        """
        Find similar names using blocking and fuzzy matching.
        
        Saves results to an Excel or CSV file if output_path is provided.
        
        :param output_path: Path to save the similarity results (default: 'final_smart_similar_names.xlsx').
                           If None, results will not be saved to file.
        :return: DataFrame of similar names.
        """
        # ساخت لیست از ایندکس‌ها و نام‌های خانوادگی برای blocking
        records = []
        for idx in self.df.index:
            first_name = self.df.loc[idx, "FirstName"]
            last_name = self.df.loc[idx, "LastName"]
            if pd.notna(first_name) and pd.notna(last_name):
                records.append((idx, first_name, last_name))

        results = []
        
        # ✅ برای مجموعه‌های کوچک (کمتر از 20 رکورد)، همه را با هم مقایسه می‌کنیم
        # این باعث می‌شود نام‌های مشابه که blocking key متفاوتی دارند هم مقایسه شوند
        if len(records) < 20:
            logging.info(f"📊 Small dataset ({len(records)} records), comparing all records (no blocking)")
            logging.info(f"📊 This ensures similar names with different blocking keys are still compared")
            # مقایسه همه با همه
            for i in tqdm(range(len(records)), desc="Matching"):
                idx1, f1, l1 = records[i]
                name1_full = f"{f1} {l1}".strip()
                
                # ساخت لیست نام‌های کامل برای matching
                other_records = [(idx, f, l) for idx, f, l in records if idx != idx1]
                other_names = [f"{f} {l}".strip() for _, f, l in other_records]
                
                if not other_names:
                    continue
                
                matches = process.extract(
                    name1_full,
                    other_names,
                    scorer=fuzz.ratio,
                    limit=10
                )
                
                for match_name, score, match_idx in matches:
                    idx2, f2, l2 = other_records[match_idx]
                    
                    if idx1 >= idx2:
                        continue
                    
                    org1 = str(self.df.loc[idx1].get("OrganizationTitle", ""))
                    org2 = str(self.df.loc[idx2].get("OrganizationTitle", ""))

                    bank1 = str(self.df.loc[idx1].get("BankTitle", ""))
                    bank2 = str(self.df.loc[idx2].get("BankTitle", ""))

                    # استخراج ستون‌های اضافی برای نام اول
                    post1 = str(self.df.loc[idx1].get("Post", ""))
                    org_type1 = str(self.df.loc[idx1].get("OrganizationTypeTitle", ""))
                    company1 = str(self.df.loc[idx1].get("CompanyTitle", ""))
                    holding1 = str(self.df.loc[idx1].get("HoldingTitle", ""))

                    # استخراج ستون‌های اضافی برای نام دوم
                    post2 = str(self.df.loc[idx2].get("Post", ""))
                    org_type2 = str(self.df.loc[idx2].get("OrganizationTypeTitle", ""))
                    company2 = str(self.df.loc[idx2].get("CompanyTitle", ""))
                    holding2 = str(self.df.loc[idx2].get("HoldingTitle", ""))

                    final_score = self.smart_score(
                        f1, l1,
                        f2, l2,
                        org1,
                        org2,
                        bank1,
                        bank2
                    )

                    if final_score >= self.settings.name_threshold:
                        # ساخت نام کامل برای نمایش
                        name1_display = f"{f1} {l1}".strip()
                        name2_display = f"{f2} {l2}".strip()
                        
                        results.append([
                            name1_display,
                            post1,
                            org1,
                            org_type1,
                            company1,
                            holding1,
                            name2_display,
                            post2,
                            org2,
                            org_type2,
                            company2,
                            holding2,
                            final_score
                        ])
        else:
            # ✅ برای مجموعه‌های بزرگ، از blocking استفاده می‌کنیم
            # اما با blocking key انعطاف‌پذیرتر (چندین key برای هر رکورد)
            logging.info(f"📊 Large dataset ({len(records)} records), using flexible blocking")
            
            # ساخت blocking groups با کلیدهای متعدد برای هر رکورد
            groups = {}
            for idx, first_name, last_name in records:
                # ایجاد چندین blocking key برای هر نام خانوادگی
                keys = []
                if last_name and len(str(last_name).strip()) >= 2:
                    last_str = str(last_name).strip()
                    # کلید بر اساس 2 کاراکتر اول
                    if len(last_str) >= 2:
                        keys.append(last_str[:2])
                    # کلید بر اساس 3 کاراکتر اول
                    if len(last_str) >= 3:
                        keys.append(last_str[:3])
                    # کلید بر اساس 2 کاراکتر آخر
                    if len(last_str) >= 2:
                        keys.append(last_str[-2:])
                
                # اضافه کردن رکورد به همه گروه‌های مربوطه
                for key in keys:
                    if key:
                        groups.setdefault(key, []).append((idx, first_name, last_name))
            
            # حذف تکراری‌ها در هر گروه
            for key in groups:
                groups[key] = list(set(groups[key]))
            
            # مقایسه درون هر گروه
            for group in tqdm(groups.values(), desc="Matching"):
                for i in range(len(group)):
                    idx1, f1, l1 = group[i]
                    
                    # ساخت نام کامل برای matching
                    name1_full = f"{f1} {l1}".strip()

                    # ساخت لیست نام‌های کامل برای matching
                    group_names = [f"{f} {l}".strip() for _, f, l in group]

                    matches = process.extract(
                        name1_full,
                        group_names,
                        scorer=fuzz.ratio,
                        limit=5
                    )

                    for match_name, score, j in matches:
                        idx2, f2, l2 = group[j]

                        if idx1 >= idx2:
                            continue

                        org1 = str(self.df.loc[idx1].get("OrganizationTitle", ""))
                        org2 = str(self.df.loc[idx2].get("OrganizationTitle", ""))

                        bank1 = str(self.df.loc[idx1].get("BankTitle", ""))
                        bank2 = str(self.df.loc[idx2].get("BankTitle", ""))

                        # استخراج ستون‌های اضافی برای نام اول
                        post1 = str(self.df.loc[idx1].get("Post", ""))
                        org_type1 = str(self.df.loc[idx1].get("OrganizationTypeTitle", ""))
                        company1 = str(self.df.loc[idx1].get("CompanyTitle", ""))
                        holding1 = str(self.df.loc[idx1].get("HoldingTitle", ""))

                        # استخراج ستون‌های اضافی برای نام دوم
                        post2 = str(self.df.loc[idx2].get("Post", ""))
                        org_type2 = str(self.df.loc[idx2].get("OrganizationTypeTitle", ""))
                        company2 = str(self.df.loc[idx2].get("CompanyTitle", ""))
                        holding2 = str(self.df.loc[idx2].get("HoldingTitle", ""))

                        final_score = self.smart_score(
                            f1, l1,
                            f2, l2,
                            org1,
                            org2,
                            bank1,
                            bank2
                        )

                        if final_score >= self.settings.name_threshold:
                            # ساخت نام کامل برای نمایش
                            name1_display = f"{f1} {l1}".strip()
                            name2_display = f"{f2} {l2}".strip()
                            
                            results.append([
                                name1_display,
                                post1,
                                org1,
                                org_type1,
                                company1,
                                holding1,
                                name2_display,
                                post2,
                                org2,
                                org_type2,
                                company2,
                                holding2,
                                final_score
                            ])
            
            # حذف تکراری‌ها (چون ممکن است یک جفت در چندین گروه باشد)
            seen_pairs = set()
            unique_results = []
            for result in results:
                # استفاده از tuple مرتب شده برای اطمینان از حذف تکراری‌ها در هر دو جهت
                pair_key = tuple(sorted([result[0], result[6]]))  # (name1, name2) sorted
                if pair_key not in seen_pairs:
                    seen_pairs.add(pair_key)
                    unique_results.append(result)
            results = unique_results

        df_result = pd.DataFrame(results, columns=[
            "نام اول",
            "پست اول",
            "سازمان اول",
            "نوع سازمان اول",
            "عنوان شرکت اول",
            "عنوان هولدینگ اول",
            "نام دوم",
            "پست دوم",
            "سازمان دوم",
            "نوع سازمان دوم",
            "عنوان شرکت دوم",
            "عنوان هولدینگ دوم",
            "درصد تشابه",
        ])

        df_result = df_result.sort_values("درصد تشابه", ascending=False)
        
        # Save to file only if output_path is provided
        if output_path:
            # Detect output format from extension
            output_ext = os.path.splitext(output_path)[1].lower()
            if output_ext == '.csv':
                df_result.to_csv(output_path, index=False, encoding='utf-8-sig')
                logging.info(f"✅ Final result saved to {output_path} (CSV format)")
            else:
                df_result.to_excel(output_path, index=False)
                logging.info(f"✅ Final result saved to {output_path} (Excel format)")
        else:
            logging.info(f"✅ Found {len(df_result)} similar name pairs (not saved to file)")

        return df_result

# ✅ ---------------- RUN ----------------
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Smart Name Processor for Persian names.")
    parser.add_argument("input_file", type=str, help="Path to the input Excel or CSV file.")
    parser.add_argument("--output_similar", type=str, default="final_smart_similar_names.xlsx", help="Path to save similar names file (Excel or CSV).")
    parser.add_argument("--name_threshold", type=float, default=0.80, help="Threshold for name similarity.")
    parser.add_argument("--last_weight", type=float, default=0.5, help="Weight for last name in scoring.")
    parser.add_argument("--first_weight", type=float, default=0.2, help="Weight for first name in scoring.")
    parser.add_argument("--org_weight", type=float, default=0.3, help="Weight for organization in scoring.")
    parser.add_argument("--min_freq", type=int, default=3, help="Minimum frequency for stop first names.")

    args = parser.parse_args()

    settings = Settings(
        name_threshold=args.name_threshold,
        last_name_weight=args.last_weight,
        first_name_weight=args.first_weight,
        org_weight=args.org_weight
    )

    processor = SmartNameProcessor(settings=settings)

    processor.load_excel(args.input_file)
    processor.process_names()

    # ✅ AUTO STOP NAMES FROM EXCEL
    processor.extract_stop_first_names(min_frequency=args.min_freq)

    processor.save(args.input_file)
    processor.find_similar_names(output_path=args.output_similar)