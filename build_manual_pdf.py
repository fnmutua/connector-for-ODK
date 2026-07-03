"""Build Connector for ODK user manual PDF from MANUAL.md with embedded screenshots."""

import os
import re

from fpdf import FPDF
from PIL import Image


ROOT = os.path.dirname(os.path.abspath(__file__))
MANUAL = os.path.join(ROOT, "MANUAL.md")
OUTPUT = os.path.join(ROOT, "Connector_for_ODK_User_Manual_v2.0.pdf")

MARGIN = 18
PAGE_W = 210
CONTENT_W = PAGE_W - MARGIN * 2
MAX_IMG_H = 95
HEADER_HEIGHT = 14  # reserved below top margin on pages 2+


def clean(text):
    text = re.sub(r"\[([^\]]+)\]\([^)]+\)", r"\1", text)
    text = text.replace("**", "").replace("`", "")
    text = text.replace("—", "-").replace("–", "-").replace("→", "->")
    text = text.replace("«", "<<").replace("»", ">>")
    text = text.replace("²", "2").replace("°", " deg").replace("·", "-")
    return text.strip()


class ManualPDF(FPDF):
    def __init__(self):
        super().__init__()
        self.set_auto_page_break(auto=True, margin=22)

    def content_top(self):
        if self.page_no() == 1:
            return MARGIN
        return MARGIN + HEADER_HEIGHT

    def add_page(self, orientation="", format="", same=False):
        super().add_page(orientation=orientation, format=format, same=same)
        self.set_y(self.content_top())

    def header(self):
        if self.page_no() == 1:
            return
        self.set_font("Helvetica", "I", 8)
        self.set_text_color(110, 110, 110)
        self.set_xy(MARGIN, 10)
        self.cell(CONTENT_W, 5, "Connector for ODK - User Manual v2.0", align="C")
        self.set_draw_color(210, 210, 210)
        self.line(MARGIN, 17, PAGE_W - MARGIN, 17)

    def footer(self):
        self.set_y(-12)
        self.set_font("Helvetica", "I", 8)
        self.set_text_color(110, 110, 110)
        self.cell(0, 8, f"Page {self.page_no()}", align="C")

    def ensure_space(self, h):
        if self.get_y() < self.content_top():
            self.set_y(self.content_top())
        if self.get_y() + h > self.h - 24:
            self.add_page()

    def heading(self, text, level):
        self.ensure_space(16 if level < 3 else 12)
        if level == 1:
            self.set_font("Helvetica", "B", 22)
            self.set_text_color(25, 73, 120)
            self.multi_cell(CONTENT_W, 10, clean(text))
            self.set_draw_color(25, 73, 120)
            self.line(MARGIN, self.get_y() + 2, PAGE_W - MARGIN, self.get_y() + 2)
            self.ln(6)
        elif level == 2:
            self.ln(3)
            self.set_font("Helvetica", "B", 14)
            self.set_text_color(25, 73, 120)
            self.multi_cell(CONTENT_W, 8, clean(text))
            self.ln(3)
        else:
            self.set_font("Helvetica", "B", 11)
            self.set_text_color(50, 50, 50)
            self.multi_cell(CONTENT_W, 6, clean(text))
            self.ln(2)
        self.set_text_color(35, 35, 35)

    def paragraph(self, text, size=10, indent=0, style=""):
        text = clean(text)
        if not text:
            self.ln(2)
            return
        self.set_font("Helvetica", style, size)
        x = self.get_x()
        if indent:
            self.set_x(MARGIN + indent)
            self.multi_cell(CONTENT_W - indent, 5, text)
            self.set_x(x)
        else:
            self.multi_cell(CONTENT_W, 5, text)
        self.ln(2)

    def bullet(self, text, numbered=False, num=""):
        prefix = f"{num}. " if numbered else "- "
        self.paragraph(prefix + text, indent=4)

    def blockquote(self, text):
        self.set_fill_color(245, 247, 250)
        self.set_font("Helvetica", "", 9)
        self.set_x(MARGIN + 4)
        self.multi_cell(CONTENT_W - 8, 5, clean(text), fill=True)
        self.ln(2)
        self.set_x(MARGIN)

    def code_block(self, text):
        text = clean(text)
        if not text:
            return
        pad = 3
        line_h = 4.8
        self.set_font("Courier", "", 8)
        lines = self.multi_cell(CONTENT_W - pad * 2, line_h, text, dry_run=True, output="LINES")
        box_h = max(line_h, len(lines) * line_h) + pad * 2
        self.ensure_space(box_h + 4)
        y0 = self.get_y()
        self.set_fill_color(248, 250, 252)
        self.set_draw_color(25, 73, 120)
        self.set_line_width(0.4)
        self.rect(MARGIN, y0, CONTENT_W, box_h, style="DF")
        self.set_xy(MARGIN + pad, y0 + pad)
        self.set_font("Courier", "", 8)
        self.set_text_color(35, 35, 35)
        self.multi_cell(CONTENT_W - pad * 2, line_h, "\n".join(lines))
        self.set_y(y0 + box_h + 3)

    def table(self, rows):
        if not rows:
            return
        cols = len(rows[0])
        col_w = CONTENT_W / cols
        line_h = 4.5
        pad = 1.5

        for i, row in enumerate(rows):
            if len(row) != cols:
                continue

            is_header = i == 0
            self.set_font("Helvetica", "B" if is_header else "", 8)

            cell_lines = []
            max_lines = 1
            for cell in row:
                txt = clean(cell)
                lines = self.multi_cell(col_w - 2, line_h, txt, dry_run=True, output="LINES")
                cell_lines.append(lines or [""])
                max_lines = max(max_lines, len(lines or [""]))

            row_h = max_lines * line_h + pad * 2
            self.ensure_space(row_h)
            y0 = self.get_y()
            x0 = MARGIN

            if is_header:
                self.set_fill_color(232, 238, 245)
            else:
                self.set_fill_color(255 if i % 2 else 250, 255, 255)

            self.set_draw_color(200, 205, 210)
            for c in range(cols):
                x = x0 + c * col_w
                self.rect(x, y0, col_w, row_h, style="DF")
                self.set_xy(x + 1, y0 + pad)
                self.set_font("Helvetica", "B" if is_header else "", 8)
                self.multi_cell(col_w - 2, line_h, "\n".join(cell_lines[c]))

            self.set_xy(MARGIN, y0 + row_h)

        self.ln(3)

    def figure_caption(self, text):
        self.set_font("Helvetica", "I", 9)
        self.set_text_color(25, 73, 120)
        self.multi_cell(CONTENT_W, 5, clean(text).strip("*"))
        self.set_text_color(35, 35, 35)
        self.ln(4)

    def embed_image(self, path):
        full = path if os.path.isabs(path) else os.path.join(ROOT, path)
        if not os.path.exists(full):
            self.paragraph(f"[Missing image: {path}]", style="I")
            return
        with Image.open(full) as img:
            w, h = img.size
        max_w = CONTENT_W - 8
        max_h = MAX_IMG_H
        scale = min(max_w / w, max_h / h, 1.0)
        disp_w = w * scale
        disp_h = h * scale
        pad = 3
        box_h = disp_h + pad * 2
        self.ensure_space(box_h + 6)
        y0 = self.get_y()
        x = MARGIN + (CONTENT_W - disp_w) / 2
        box_x = x - pad
        box_w = disp_w + pad * 2

        self.set_fill_color(248, 250, 252)
        self.set_draw_color(25, 73, 120)
        self.set_line_width(0.5)
        self.rect(box_x, y0, box_w, box_h, style="DF")

        self.image(full, x=x, y=y0 + pad, w=disp_w, h=disp_h)
        self.set_y(y0 + box_h + 2)


def parse_table(lines, start):
    rows = []
    i = start
    while i < len(lines) and lines[i].strip().startswith("|"):
        cells = [c.strip() for c in lines[i].strip().strip("|").split("|")]
        if not all(re.fullmatch(r":?-{3,}:?", c.replace(" ", "")) for c in cells):
            rows.append(cells)
        i += 1
    return rows, i


def build():
    with open(MANUAL, encoding="utf-8") as f:
        lines = f.read().splitlines()

    pdf = ManualPDF()
    pdf.set_margins(MARGIN, MARGIN, MARGIN)
    pdf.add_page()

    i = 0
    in_code = False
    code_buf = []
    skip_checklist = False

    while i < len(lines):
        raw = lines[i]
        line = raw.strip()

        if line == "## Screenshot checklist":
            break

        if line.startswith("```"):
            if in_code:
                pdf.code_block("\n".join(code_buf))
                code_buf = []
                in_code = False
            else:
                in_code = True
            i += 1
            continue

        if in_code:
            code_buf.append(line)
            i += 1
            continue

        if not line or line == "---":
            i += 1
            continue

        if line.startswith("!["):
            m = re.match(r"!\[([^\]]*)\]\(([^)]+)\)", line)
            if m:
                pdf.embed_image(m.group(2))
            i += 1
            continue

        if line.startswith("# "):
            pdf.heading(line[2:], 1)
            i += 1
            continue
        if line.startswith("## "):
            pdf.heading(line[3:], 2)
            i += 1
            continue
        if line.startswith("### "):
            pdf.heading(line[4:], 3)
            i += 1
            continue

        if line.startswith("|"):
            rows, i = parse_table(lines, i)
            pdf.table(rows)
            continue

        if line.startswith(">"):
            pdf.blockquote(line.lstrip("> "))
            i += 1
            continue

        if re.match(r"^\d+\.\s+", line):
            m = re.match(r"^(\d+)\.\s+(.*)", line)
            pdf.bullet(m.group(2), numbered=True, num=m.group(1))
            i += 1
            continue

        if line.startswith("- "):
            pdf.bullet(line[2:])
            i += 1
            continue

        if line.startswith("*Figure"):
            pdf.figure_caption(line)
            i += 1
            continue

        pdf.paragraph(line)
        i += 1

    pdf.output(OUTPUT)
    print(f"Created: {OUTPUT}")


if __name__ == "__main__":
    build()
