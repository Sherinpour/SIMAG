import pandas as pd
from hazm import Normalizer
from negar.virastar import PersianEditor
from difflib import SequenceMatcher
from itertools import combinations
from tqdm import tqdm
from rapidfuzz import fuzz

tqdm.pandas()


class NameProcessor:
    """
    A class to process Persian names by removing prefixes, correcting text,
    and finding similar names.
    """

    def __init__(self):
        """Initialize the NameProcessor with required components."""
        self.normalizer = Normalizer()

        # List of multi-word prefixes (including variants without spaces)
        self.multi_word_prefixes = [
            "آقای دکتر",
            "خانم دکتر",
            "آقای مهندس",
            "خانم مهندس",
            "جناب آقای",
            "سرکار خانم",
            "حاج آقا",
            "حاج خانم",
            "آقای استاد",
            "خانم استاد",
            "آقای پروفسور",
            "خانم پروفسور",
            "جناب آقای دکتر",
            "سرکار خانم دکتر",
            "جناب آقای مهندس",
            "سرکار خانم مهندس",
            "جناب آقای استاد",
            "سرکار خانم استاد",
            "جناب آقای پروفسور",
            "سرکار خانم پروفسور",
        ]

        # List of combined prefixes without spaces (e.g., "سرکارخانم")
        self.multi_word_prefixes_no_space = [
            "سرکارخانم",
            "جنابآقای",
            "حاجآقا",
            "حاجخانم",
        ]

        self.single_prefixes = [
            "آقا",
            "اقای",
            "آقای",
            "خانم",
            "دکتر",
            "مهندس",
            "استاد",
            "حاج",
            "حاجی",
            "کربلایی",
            "جناب",
            "سرکار",
            "پروفسور",
            "سید",
            "سیده",
            "بانو",
            "میر",
            "ملا",
            "حجت‌الاسلام",
            "آیت‌الله",
            "خان",
            "مشهدی",
        ]

        self.df = None
        self.name_column = "full_name"
        self.corrected_column = "corrected"

    def remove_prefix(self, full_name):
        """
        Remove prefixes from Persian names.

        Args:
            full_name: The full name string

        Returns:
            Name without prefixes
        """
        if pd.isna(full_name) or not full_name:
            return full_name

        # Use normalizer that was created earlier
        full_name = self.normalizer.normalize(str(full_name)).strip()

        # Remove multi-word prefixes with spaces
        changed = True
        while changed:
            changed = False
            for prefix in sorted(self.multi_word_prefixes, key=len, reverse=True):
                if full_name.startswith(prefix + " ") or full_name == prefix:
                    full_name = full_name[len(prefix) :].lstrip()
                    changed = True
                    break

        # Remove multi-word prefixes without spaces (e.g., "سرکارخانم")
        changed = True
        while changed:
            changed = False
            for prefix in sorted(
                self.multi_word_prefixes_no_space, key=len, reverse=True
            ):
                if full_name.startswith(prefix):
                    # Check that after prefix a Persian character starts
                    remaining = full_name[len(prefix) :]
                    if remaining and (
                        remaining[0].isalpha()
                        or remaining[0] in "آابپتثجچحخدذرزژسشصضطظعغفقکگلمنوهی"
                    ):
                        full_name = remaining
                        changed = True
                        break

        # Remove single-word prefixes that are separated by spaces
        words = full_name.split()
        i = 0
        while i < len(words) and words[i] in self.single_prefixes:
            i += 1

        # If all words were removed, return the original text
        if i >= len(words):
            return full_name

        result = " ".join(words[i:])

        # Remove single-word prefixes that are attached without space (e.g., "دکترنازنین")
        for prefix in sorted(self.single_prefixes, key=len, reverse=True):
            if result.startswith(prefix):
                # Check that after prefix a Persian character starts
                remaining = result[len(prefix) :]
                if remaining and (
                    remaining[0].isalpha()
                    or remaining[0] in "آابپتثجچحخدذرزژسشصضطظعغفقکگلمنوهی"
                ):
                    # Only if prefix is at the beginning of name (not in middle)
                    if len(remaining) > 0:
                        result = remaining
                        break

        return result.strip()

    def correct_with_negar(self, name):
        """
        Correct Persian text using PersianEditor.

        Args:
            name: The name string to correct

        Returns:
            Corrected name
        """
        if pd.isna(name) or not name:
            return name
        # Create PersianEditor instance for each name (required because PersianEditor is created with text)
        editor = PersianEditor(str(name))
        return editor.cleanup()

    def load_excel(self, file_path, name_column="FullName"):
        """
        Load Excel file and set the name column.
        Creates a full_name column by combining FirstName and LastName.

        Args:
            file_path: Path to the Excel file
            name_column: Name of the column containing full names (default: "full_name")
        """
        print(f"Loading Excel file: {file_path}")
        self.df = pd.read_excel(file_path)

        # Create full_name column by combining FirstName and LastName
        if "FirstName" in self.df.columns and "LastName" in self.df.columns:
            self.df["full_name"] = (
                self.df["FirstName"].fillna("").astype(str).str.strip()
                + " "
                + self.df["LastName"].fillna("").astype(str).str.strip()
            )
            self.df["full_name"] = self.df["full_name"].str.strip()
            print(f"Created 'full_name' column from FirstName and LastName")
        else:
            print(
                "Warning: FirstName and/or LastName columns not found. Skipping full_name creation."
            )

        self.name_column = name_column
        print(f"Loaded {len(self.df)} rows")
        return self.df

    def process_names(self):
        """
        Process names by removing prefixes and correcting text.
        Overwrites the original name column with corrected names.
        """
        if self.df is None:
            raise ValueError("DataFrame not loaded. Please call load_excel() first.")

        print("Processing names...")

        # Remove prefixes
        print("Step 1: Removing prefixes...")
        self.df[self.name_column] = self.df[self.name_column].progress_apply(
            self.remove_prefix
        )

        # Correct with PersianEditor
        print("\nStep 2: Correcting text with PersianEditor...")
        self.df[self.name_column] = self.df[self.name_column].progress_apply(
            self.correct_with_negar
        )

        print(f"\n✅ Processed {len(self.df)} rows.")
        return self.df

    def save_to_excel(self, output_path):
        """
        Save the processed dataframe to Excel file.

        Args:
            output_path: Path to save the Excel file
        """
        if self.df is None:
            raise ValueError("No data to save. Please process names first.")

        print(f"\nSaving to Excel file: {output_path}")
        self.df.to_excel(output_path, index=False, engine="openpyxl")
        print(f"✅ Results saved to Excel file.")

    def find_similar_names(self, similarity_threshold=0.8, use_filter=True):
        if self.df is None or self.name_column not in self.df.columns:
            raise ValueError("Names not processed. Please call process_names() first.")

        all_names_with_indices = []
        for idx, name in self.df[self.name_column].items():
            if pd.notna(name) and str(name).strip():
                all_names_with_indices.append((idx, str(name).strip()))

        print(f"Total rows to check: {len(all_names_with_indices)}")

        def split_name(name):
            words = name.split()
            if len(words) == 0:
                return "", ""
            elif len(words) == 1:
                return "", words[0]
            else:
                first_name = " ".join(words[:-1])
                last_name = words[-1]
                return first_name, last_name

        def similarity(a, b):
            return fuzz.ratio(a, b) / 100.0

        def should_filter(name1, name2):
            first1, last1 = split_name(name1)
            first2, last2 = split_name(name2)

            if not first1 or not first2 or not last1 or not last2:
                return False

            first_sim = similarity(first1, first2)
            last_sim = similarity(last1, last2)

            if first_sim > 0.85 and last_sim < 0.7:
                return True

            if first_sim < 0.8 and last_sim > 0.85:
                return True

            if 0.7 <= first_sim <= 0.85 and last_sim < 0.6:
                return True

            return False

        org_position_col = "Post"
        org_name_col = "OrganizationTitle"
        bank_name_col = "BankTitle"

        has_org_position = org_position_col in self.df.columns
        has_org_name = org_name_col in self.df.columns
        has_bank_name = bank_name_col in self.df.columns

        seen_pairs = set()
        initial_pairs = []

        for i in tqdm(range(len(all_names_with_indices)), desc="Calculating similarities"):
            idx1, name1 = all_names_with_indices[i]
            for j in range(i + 1, len(all_names_with_indices)):
                idx2, name2 = all_names_with_indices[j]

                if name1 == name2:
                    continue

                sim = similarity(name1, name2)

                if sim > similarity_threshold:
                    if use_filter and should_filter(name1, name2):
                        continue

                    pair_key = (min(idx1, idx2), max(idx1, idx2))
                    if pair_key in seen_pairs:
                        continue
                    seen_pairs.add(pair_key)

                    excel_row1 = idx1 + 2
                    excel_row2 = idx2 + 2

                    initial_pairs.append(
                        (excel_row1, name1, excel_row2, name2, round(sim, 2), idx1, idx2)
                    )

        print(f"\nFound {len(initial_pairs)} initial similar pairs before additional filter.")

        pairs = []

        for pair in initial_pairs:
            excel_row1, name1, excel_row2, name2, sim, idx1, idx2 = pair

            # ✅ گرفتن نام سازمان‌ها
            org_name1 = (
                str(self.df.loc[idx1, org_name_col]).strip()
                if has_org_name and pd.notna(self.df.loc[idx1, org_name_col])
                else ""
            )

            org_name2 = (
                str(self.df.loc[idx2, org_name_col]).strip()
                if has_org_name and pd.notna(self.df.loc[idx2, org_name_col])
                else ""
            )

            # ✅ محاسبه درصد تشابه سازمان
            if org_name1 and org_name2:
                org_similarity = round(similarity(org_name1, org_name2), 2)
            else:
                org_similarity = ""

            org_name_passed = False
            if org_name1 and org_name2:
                if similarity(org_name1, org_name2) >= 0.5:
                    org_name_passed = True
            else:
                org_name_passed = True

            if org_name_passed:
                pairs.append(
                    (
                        excel_row1,
                        name1,
                        excel_row2,
                        name2,
                        sim,
                        org_name1,
                        org_name2,
                        org_similarity,
                    )
                )
                continue

            bank_name_passed = False
            if has_bank_name:
                bank_name1 = str(self.df.loc[idx1, bank_name_col]).strip()
                bank_name2 = str(self.df.loc[idx2, bank_name_col]).strip()

                if bank_name1 and bank_name2:
                    if similarity(bank_name1, bank_name2) >= 0.5:
                        bank_name_passed = True
                else:
                    bank_name_passed = True
            else:
                bank_name_passed = True

            if bank_name_passed:
                pairs.append(
                    (
                        excel_row1,
                        name1,
                        excel_row2,
                        name2,
                        sim,
                        org_name1,
                        org_name2,
                        org_similarity,
                    )
                )
                continue

            if has_org_position:
                org_pos1 = str(self.df.loc[idx1, org_position_col]).strip()
                org_pos2 = str(self.df.loc[idx2, org_position_col]).strip()

                if org_pos1 and org_pos2:
                    if similarity(org_pos1, org_pos2) >= 0.6:
                        pairs.append(
                            (
                                excel_row1,
                                name1,
                                excel_row2,
                                name2,
                                sim,
                                org_name1,
                                org_name2,
                                org_similarity,
                            )
                        )
                else:
                    pairs.append(
                        (
                            excel_row1,
                            name1,
                            excel_row2,
                            name2,
                            sim,
                            org_name1,
                            org_name2,
                            org_similarity,
                        )
                    )
            else:
                pairs.append(
                    (
                        excel_row1,
                        name1,
                        excel_row2,
                        name2,
                        sim,
                        org_name1,
                        org_name2,
                        org_similarity,
                    )
                )

        print(f"After additional filter, {len(pairs)} pairs remain.")

        df_similar = pd.DataFrame(
            pairs,
            columns=[
                "Excel Row 1",
                "نام اول",
                "Excel Row 2",
                "نام دوم",
                "درصد تشابه اسم",
                "سازمان 1",
                "سازمان 2",
                "درصد تشابه سازمان",
            ],
        )

        df_similar = df_similar.sort_values("درصد تشابه اسم", ascending=False)

        if not df_similar.empty:
            print(df_similar)
            df_similar.to_excel(
                "final_similar_names.xlsx", index=False, engine="openpyxl"
            )
            print("✅ Final pairs saved to 'final_similar_names.xlsx'.")
        else:
            print("❌ No similar names found.")

        return df_similar


# Example usage
if __name__ == "__main__":
    # Create processor instance
    processor = NameProcessor()

    # Load Excel file
    processor.load_excel("/home/sherin/SIMAG/LastGuests.xlsx")

    # Process names
    processor.process_names()

    # Save to Excel
    processor.save_to_excel("/home/sherin/SIMAG/LastGuests.xlsx")

    # Find similar names
    processor.find_similar_names(similarity_threshold=0.8)
