import pandas as pd
import re
import logging
from dataclasses import dataclass
from hazm import Normalizer
from negar.virastar import PersianEditor
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
    A class for processing and matching Persian names from an Excel file.
    
    This class loads an Excel file, cleans and normalizes names, extracts common first names,
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
        Correct Persian text using PersianEditor for cleanup.
        
        :param name: The name string to correct.
        :return: Corrected name.
        """
        if not name:
            return name
        editor = PersianEditor(str(name))
        return editor.cleanup()

    # ✅ 3. LOAD EXCEL
    def load_excel(self, file_path):
        """
        Load the Excel file and check for required columns.
        
        Checks for required columns FirstName and LastName.
        
        :param file_path: Path to the Excel file.
        :return: Loaded DataFrame.
        """
        logging.info(f"Loading file: {file_path}")
        self.df = pd.read_excel(file_path)

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

        self.df["FirstName"] = self.df["FirstName"].progress_apply(self.remove_prefix_fast)
        self.df["FirstName"] = self.df["FirstName"].progress_apply(self.correct_text_fast)
        
        self.df["LastName"] = self.df["LastName"].progress_apply(self.remove_prefix_fast)
        self.df["LastName"] = self.df["LastName"].progress_apply(self.correct_text_fast)

        logging.info("Names normalized & corrected.")

    # ✅ 5. SAVE
    def save(self, output_path):
        """
        Save the processed DataFrame to an Excel file.
        
        :param output_path: Path to save the output Excel file.
        """
        self.df.to_excel(output_path, index=False)
        logging.info(f"Saved to {output_path}")

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
        first_names = self.df["FirstName"].dropna().astype(str).str.strip()
        first_names = first_names[first_names != ""]

        freq = first_names.value_counts()

        self.settings.stop_first_names = list(
            freq[freq >= min_frequency].index
        )

        logging.info(
            f"✅ Auto stop_first_names extracted: {self.settings.stop_first_names}"
        )

    # ✅ 8. FAST MATCHING ENGINE
    def find_similar_names(self, output_path="final_smart_similar_names.xlsx"):
        """
        Find similar names using blocking and fuzzy matching.
        
        Saves results to an Excel file.
        
        :param output_path: Path to save the similarity results (default: 'final_smart_similar_names.xlsx').
        :return: DataFrame of similar names.
        """
        # ساخت لیست از ایندکس‌ها و نام‌های خانوادگی برای blocking
        records = []
        for idx in self.df.index:
            first_name = self.df.loc[idx, "FirstName"]
            last_name = self.df.loc[idx, "LastName"]
            if pd.notna(first_name) and pd.notna(last_name):
                records.append((idx, first_name, last_name))

        # ✅ Blocking بر اساس نام خانوادگی
        groups = {}
        for idx, first_name, last_name in records:
            key = self.blocking_key(last_name)
            if key:
                groups.setdefault(key, []).append((idx, first_name, last_name))

        results = []

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
        df_result.to_excel(output_path, index=False)

        logging.info(f"✅ Final result saved to {output_path}")
        return df_result

# ✅ ---------------- RUN ----------------
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Smart Name Processor for Persian names.")
    parser.add_argument("input_file", type=str, help="Path to the input Excel file.")
    parser.add_argument("--output_similar", type=str, default="final_smart_similar_names.xlsx", help="Path to save similar names Excel file.")
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