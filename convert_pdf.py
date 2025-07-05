#!/usr/bin/env python3
"""
Charter Schools PDF to CSV Extractor

This script extracts charter school information from a PDF document and converts
it to a structured CSV file suitable for use with Apache Spark or pandas.

Requirements:
    pip install PyPDF2 pandas regex

Usage:
    python pdf_extractor.py input.pdf output.csv

Example:
    python pdf_extractor.py charter_schools.pdf illinois_charter_schools.csv
"""

import re
import csv
import sys
import pandas as pd
from typing import List, Dict, Optional
import PyPDF2
from pathlib import Path


class CharterschoolExtractor:
    """
    Extracts charter school information from PDF documents.

    The extractor handles the specific format used by Illinois Network of Charter Schools
    where each school entry contains:
    - School Name
    - Address (Street, City, State ZIP)
    - Phone Number
    - Charter Type
    - Grade Levels
    - SQRP Rating
    - School Profile URL
    """

    def __init__(self):
        # Regex patterns for extracting different components
        self.patterns = {
            'phone': r'\(\d{3}\)\s\d{3}-\d{4}',
            'address': r'^\d+.+?IL\s\d{5}(?:-\d{4})?$',
            'url': r'https://www\.incschools\.org/school/[^/\s]+/?',
            'grade_levels': r'^(?:PK\s?-\s?\d+|K\s?-\s?\d+|\d+\s?-\s?\d+|N/A)$',
            'sqrp_rating': r'^(?:Level\s\d\+?|Not Applicable|Inability to Rate)$'
        }

    def extract_text_from_pdf(self, pdf_path: str) -> str:
        """
        Extract all text from PDF file.

        Args:
            pdf_path (str): Path to the PDF file

        Returns:
            str: Concatenated text from all pages

        Example:
            >>> extractor = CharterschoolExtractor()
            >>> text = extractor.extract_text_from_pdf("schools.pdf")
        """
        text = ""
        try:
            with open(pdf_path, 'rb') as file:
                pdf_reader = PyPDF2.PdfReader(file)

                for page_num, page in enumerate(pdf_reader.pages):
                    try:
                        page_text = page.extract_text()
                        text += page_text + "\n"
                        print(f"Processed page {page_num + 1}")
                    except Exception as e:
                        print(f"Error processing page {page_num + 1}: {e}")
                        continue

        except Exception as e:
            print(f"Error reading PDF: {e}")
            sys.exit(1)

        return text

    def clean_text_lines(self, text: str) -> List[str]:
        """
        Clean and filter text lines, removing headers, footers, and empty lines.

        Args:
            text (str): Raw text from PDF

        Returns:
            List[str]: Cleaned lines of text

        Example:
            >>> lines = extractor.clean_text_lines(raw_text)
            >>> print(f"Found {len(lines)} lines")
        """
        lines = text.split('\n')
        cleaned_lines = []

        for line in lines:
            line = line.strip()

            # Skip empty lines and common headers/footers
            if not line:
                continue
            if 'Privacy - Terms' in line:
                continue
            if 'Find a Charter School' in line:
                continue
            if 'Illinois Network of Charter Schools' in line:
                continue
            if re.match(r'^\d+/\d+$', line):  # Page numbers like "1/20"
                continue
            if 'https://www.incschools.org/find-a-charter-school/?' in line:
                continue
            if line.startswith('7/4/25'):  # Date headers
                continue
            if 'Results' in line and line.count(' ') < 3:
                continue

            cleaned_lines.append(line)

        return cleaned_lines

    def identify_line_type(self, line: str) -> str:
        """
        Identify what type of information a line contains.

        Args:
            line (str): Single line of text

        Returns:
            str: Type identifier ('phone', 'address', 'url', etc.)

        Example:
            >>> line_type = extractor.identify_line_type("(773) 582-1100")
            >>> print(line_type)  # Output: 'phone'
        """
        # Check against each pattern
        if re.search(self.patterns['phone'], line):
            return 'phone'
        elif re.match(self.patterns['address'], line):
            return 'address'
        elif re.match(self.patterns['url'], line):
            return 'url'
        elif line == 'Charter':
            return 'charter_type'
        elif re.match(self.patterns['grade_levels'], line):
            return 'grade_levels'
        elif re.match(self.patterns['sqrp_rating'], line):
            return 'sqrp_rating'
        elif line.startswith('SQRP Rating:'):
            return 'sqrp_rating_line'
        elif line.startswith('School Profile:'):
            return 'profile_line'
        else:
            return 'unknown'

    def extract_schools_data(self, lines: List[str]) -> List[Dict[str, str]]:
        """
        Extract structured school data from cleaned text lines.

        Args:
            lines (List[str]): Cleaned lines from PDF

        Returns:
            List[Dict[str, str]]: List of school dictionaries

        Example:
            >>> schools = extractor.extract_schools_data(cleaned_lines)
            >>> print(f"Extracted {len(schools)} schools")
            >>> print(schools[0]['School_Name'])
        """
        schools = []
        current_school = {}

        i = 0
        while i < len(lines):
            line = lines[i]
            line_type = self.identify_line_type(line)

            # If we encounter what looks like a school name (not matching any pattern)
            if line_type == 'unknown' and len(line) > 10 and not line.isupper():
                # Save previous school if complete
                if self._is_school_complete(current_school):
                    schools.append(current_school.copy())

                # Start new school
                current_school = {
                    'School_Name': line,
                    'Address': '',
                    'Phone_Number': '',
                    'Charter_Type': 'Charter',
                    'Grade_Levels': '',
                    'SQRP_Rating': '',
                    'School_Profile_URL': ''
                }

            elif line_type == 'address':
                current_school['Address'] = line

            elif line_type == 'phone':
                current_school['Phone_Number'] = line

            elif line_type == 'grade_levels':
                current_school['Grade_Levels'] = line

            elif line_type == 'sqrp_rating':
                current_school['SQRP_Rating'] = line

            elif line_type == 'sqrp_rating_line':
                # Extract rating from "SQRP Rating: Level 2+" format
                rating = line.replace('SQRP Rating:', '').strip()
                current_school['SQRP_Rating'] = rating

            elif line_type == 'url':
                current_school['School_Profile_URL'] = line

            elif line_type == 'profile_line':
                # Extract URL from "School Profile: https://..." format
                url = line.replace('School Profile:', '').strip()
                current_school['School_Profile_URL'] = url

            i += 1

        # Don't forget the last school
        if self._is_school_complete(current_school):
            schools.append(current_school)

        return schools

    def _is_school_complete(self, school: Dict[str, str]) -> bool:
        """
        Check if a school dictionary has minimum required fields.

        Args:
            school (Dict[str, str]): School data dictionary

        Returns:
            bool: True if school has essential information
        """
        return (school.get('School_Name', '') != '' and
                school.get('Address', '') != '')

    def save_to_csv(self, schools: List[Dict[str, str]], output_path: str) -> None:
        """
        Save schools data to CSV file.

        Args:
            schools (List[Dict[str, str]]): List of school dictionaries
            output_path (str): Path for output CSV file

        Example:
            >>> extractor.save_to_csv(schools_data, "output.csv")
            >>> print("CSV file saved successfully!")
        """
        if not schools:
            print("No schools data to save!")
            return

        # Create DataFrame and save to CSV
        df = pd.DataFrame(schools)

        # Ensure column order
        column_order = [
            'School_Name', 'Address', 'Phone_Number',
            'Charter_Type', 'Grade_Levels', 'SQRP_Rating',
            'School_Profile_URL'
        ]

        # Reorder columns, keeping any extra columns at the end
        available_cols = [col for col in column_order if col in df.columns]
        extra_cols = [col for col in df.columns if col not in column_order]
        final_cols = available_cols + extra_cols

        df = df[final_cols]

        # Save to CSV
        df.to_csv(output_path, index=False, quoting=csv.QUOTE_ALL)
        print(f"Successfully saved {len(schools)} schools to {output_path}")

        # Print summary statistics
        self._print_summary_stats(df)

    def _print_summary_stats(self, df: pd.DataFrame) -> None:
        """Print summary statistics about the extracted data."""
        print("\n=== EXTRACTION SUMMARY ===")
        print(f"Total schools: {len(df)}")
        print(f"Schools with phone numbers: {df['Phone_Number'].str.len().gt(0).sum()}")
        print(f"Schools with SQRP ratings: {df['SQRP_Rating'].str.len().gt(0).sum()}")

        # Grade level distribution
        print("\nGrade Level Distribution:")
        grade_counts = df['Grade_Levels'].value_counts().head(10)
        for grade, count in grade_counts.items():
            print(f"  {grade}: {count}")

        # SQRP Rating distribution
        print("\nSQRP Rating Distribution:")
        rating_counts = df['SQRP_Rating'].value_counts()
        for rating, count in rating_counts.items():
            print(f"  {rating}: {count}")


def main():
    """
    Main function to run the PDF extraction process.

    Usage:
        python script.py input.pdf output.csv

    Example:
        python pdf_extractor.py charter_schools.pdf illinois_schools.csv
    """
    if len(sys.argv) != 3:
        print("Usage: python pdf_extractor.py <input_pdf> <output_csv>")
        print("Example: python pdf_extractor.py schools.pdf output.csv")
        sys.exit(1)

    input_pdf = sys.argv[1]
    output_csv = sys.argv[2]

    # Validate input file exists
    if not Path(input_pdf).exists():
        print(f"Error: Input file '{input_pdf}' not found!")
        sys.exit(1)

    print(f"Starting extraction from {input_pdf}...")

    # Initialize extractor and process
    extractor = CharterschoolExtractor()

    # Step 1: Extract text from PDF
    print("Step 1: Extracting text from PDF...")
    raw_text = extractor.extract_text_from_pdf(input_pdf)

    # Step 2: Clean and prepare text lines
    print("Step 2: Cleaning text lines...")
    lines = extractor.clean_text_lines(raw_text)
    print(f"Found {len(lines)} lines to process")

    # Step 3: Extract structured school data
    print("Step 3: Extracting school data...")
    schools_data = extractor.extract_schools_data(lines)

    # Step 4: Save to CSV
    print("Step 4: Saving to CSV...")
    extractor.save_to_csv(schools_data, output_csv)

    print(f"\n✅ Extraction complete! Output saved to: {output_csv}")


# Alternative function for direct use in scripts/notebooks
def extract_schools_from_pdf(pdf_path: str, csv_path: str) -> pd.DataFrame:
    """
    Convenience function to extract schools data and return as DataFrame.

    Args:
        pdf_path (str): Path to input PDF file
        csv_path (str): Path for output CSV file

    Returns:
        pd.DataFrame: Extracted schools data

    Example:
        >>> df = extract_schools_from_pdf("schools.pdf", "output.csv")
        >>> print(df.head())
        >>> print(f"Found {len(df)} schools")
    """
    extractor = CharterschoolExtractor()

    raw_text = extractor.extract_text_from_pdf(pdf_path)
    lines = extractor.clean_text_lines(raw_text)
    schools_data = extractor.extract_schools_data(lines)
    extractor.save_to_csv(schools_data, csv_path)

    return pd.DataFrame(schools_data)


if __name__ == "__main__":
    main()


# Example usage in a Jupyter notebook or script:
"""
# For interactive use:
from pdf_extractor import extract_schools_from_pdf

# Extract data
df = extract_schools_from_pdf("charter_schools.pdf", "output.csv")

# Analyze the data
print(df.head())
print(df['SQRP_Rating'].value_counts())

# Filter for specific criteria
high_schools = df[df['Grade_Levels'].str.contains('12', na=False)]
level_2_schools = df[df['SQRP_Rating'].str.contains('Level 2', na=False)]

# Use with Spark
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("CharterSchools").getOrCreate()
spark_df = spark.read.option("header", "true").csv("output.csv")
spark_df.show()
"""
