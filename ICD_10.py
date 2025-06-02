import os
import re
import pytesseract # type: ignore
import polars as pl
from pdf2image import convert_from_path # type: ignore
from pathlib import Path

# OCR-based extraction from image-based PDF
def extract_conditions_icd10_from_image_pdf(pdf_path):
    pages = convert_from_path(pdf_path, dpi=300)
    all_text = "\n".join(pytesseract.image_to_string(page) for page in pages)

    # Split blocks by 2+ newlines (assumes one condition per paragraph-ish block)
    blocks = re.split(r"\n{2,}", all_text)
    code_pattern = re.compile(r"\b[A-TV-Z]\d{2}(?:\.[0-9A-Z]{1,4})?\b")

    data = []
    for block in blocks:
        lines = [line.strip() for line in block.split("\n") if line.strip()]
        if not lines:
            continue
        # Heuristic: First line = condition name, rest = description/codes
        condition = lines[0]
        code_lines = lines[1:]
        codes = set()
        for line in code_lines:
            codes.update(code_pattern.findall(line))
        if codes:
            data.append({
                "Condition": condition,
                "ICD_10_Codes": ", ".join(sorted(codes))
            })
    return data

# Save to Excel or CSV via Polars
def save_to_excel(data, output_path):
    df = pl.DataFrame(data)
    output_path = Path(output_path)
    if output_path.suffix == ".xlsx":
        df.to_pandas().to_excel(output_path, index=False)
    else:
        df.write_csv(output_path)
    print(f" File saved to: {output_path}")

if __name__ == "__main__":
    input_pdf = "/Users/krithik/Desktop/chr-chronic-condition-algorithms_2025 2.pdf"
    output_file = "/Users/krithik/Desktop/chronic_conditions_ocr.xlsx"

    if not os.path.exists(input_pdf):
        print(f" File not found: {input_pdf}")
    else:
        print(" Extracting using OCR...")
        extracted_data = extract_conditions_icd10_from_image_pdf(input_pdf)
        print(f" Found {len(extracted_data)} conditions with codes.")
        save_to_excel(extracted_data, output_file)

