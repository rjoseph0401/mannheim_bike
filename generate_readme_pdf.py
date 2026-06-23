"""Generiert Results/README.pdf aus README.md mit fpdf2."""
from pathlib import Path
from fpdf import FPDF

README = Path("README.md")
OUT    = Path("Results/README.pdf")

FONT_DIR  = Path("C:/Windows/Fonts")
FONT_REG  = FONT_DIR / "arial.ttf"
FONT_BOLD = FONT_DIR / "arialbd.ttf"
FONT_MONO = FONT_DIR / "lucon.ttf"   # Lucida Console – kein Namenskonflikt

SECTION_COLOR = (33, 92, 166)
TABLE_HEADER  = (220, 230, 245)
CODE_BG       = (245, 245, 245)
LEFT_MARGIN   = 18
RIGHT_MARGIN  = 18


class PDF(FPDF):
    def header(self):
        self.set_font("Sans", "B", 8)
        self.set_text_color(120, 120, 120)
        self.cell(0, 6, "Fahrradverkehr Mannheim – Dokumentation", align="R")
        self.ln(4)

    def footer(self):
        self.set_y(-12)
        self.set_font("Sans", "", 8)
        self.set_text_color(120, 120, 120)
        self.cell(0, 6, f"Seite {self.page_no()}", align="C")


def flush_table(pdf: PDF, rows: list, col_chars: list) -> None:
    if not rows:
        return
    usable = pdf.epw
    total  = sum(col_chars) or 1
    widths = [max(c / total * usable, 8) for c in col_chars]

    for i, row in enumerate(rows):
        if pdf.get_y() + 7 > pdf.page_break_trigger:
            pdf.add_page()
        is_hdr = (i == 0)
        pdf.set_fill_color(*(TABLE_HEADER if is_hdr else (255, 255, 255)))
        pdf.set_font("Sans", "B" if is_hdr else "", 8)
        pdf.set_text_color(30, 30, 30)
        for j, cell in enumerate(row):
            w = widths[j] if j < len(widths) else 20
            pdf.cell(w, 6, cell.strip(), border=1, fill=True)
        pdf.ln()
    pdf.ln(2)


def text_plain(s: str) -> str:
    """Strip markdown bold markers."""
    return s.replace("**", "").replace("__", "")


pdf = PDF()
pdf.add_font("Sans", "",  str(FONT_REG),  )
pdf.add_font("Sans", "B", str(FONT_BOLD), )
pdf.add_font("Mono", "",  str(FONT_MONO), )
pdf.set_auto_page_break(auto=True, margin=16)
pdf.add_page()
pdf.set_margins(LEFT_MARGIN, 18, RIGHT_MARGIN)

lines = README.read_text(encoding="utf-8").splitlines()

in_table = False
in_code  = False
table_rows: list = []
col_chars: list  = []

for raw_line in lines:
    line = raw_line.rstrip()

    # ── code block ──────────────────────────────────────────
    if line.startswith("```"):
        if not in_code:
            in_code = True
            pdf.ln(1)
        else:
            in_code = False
            pdf.ln(2)
        continue

    if in_code:
        pdf.set_font("Mono", "", 8)
        pdf.set_text_color(40, 40, 40)
        pdf.set_fill_color(*CODE_BG)
        pdf.set_x(LEFT_MARGIN)
        pdf.multi_cell(pdf.epw, 5, line if line else " ", fill=True)
        continue

    # ── table ───────────────────────────────────────────────
    if line.startswith("|"):
        cells = [c for c in line.split("|") if c != ""]
        if all(set(c.strip()) <= {"-", ":"} for c in cells):
            continue  # separator row
        in_table = True
        clean = [text_plain(c) for c in cells]
        table_rows.append(clean)
        if not col_chars:
            col_chars = [max(6, len(c.strip())) for c in clean]
        else:
            col_chars = [max(col_chars[i] if i < len(col_chars) else 0,
                             len(c.strip()))
                         for i, c in enumerate(clean)]
        continue
    else:
        if in_table:
            flush_table(pdf, table_rows, col_chars)
            in_table = False
            table_rows = []
            col_chars  = []

    # ── headings ────────────────────────────────────────────
    if line.startswith("### "):
        pdf.ln(3)
        pdf.set_font("Sans", "B", 11)
        pdf.set_text_color(*SECTION_COLOR)
        pdf.set_x(LEFT_MARGIN)
        pdf.multi_cell(pdf.epw, 7, line[4:])
        pdf.ln(1)
        pdf.set_text_color(0, 0, 0)
        continue

    if line.startswith("## "):
        pdf.ln(4)
        pdf.set_font("Sans", "B", 13)
        pdf.set_text_color(*SECTION_COLOR)
        pdf.set_x(LEFT_MARGIN)
        pdf.multi_cell(pdf.epw, 8, line[3:])
        y = pdf.get_y()
        pdf.set_draw_color(*SECTION_COLOR)
        pdf.line(LEFT_MARGIN, y, pdf.w - RIGHT_MARGIN, y)
        pdf.ln(3)
        pdf.set_text_color(0, 0, 0)
        continue

    if line.startswith("# "):
        pdf.set_font("Sans", "B", 16)
        pdf.set_text_color(*SECTION_COLOR)
        pdf.set_x(LEFT_MARGIN)
        pdf.multi_cell(pdf.epw, 10, line[2:])
        pdf.ln(2)
        pdf.set_text_color(0, 0, 0)
        continue

    # ── horizontal rule ─────────────────────────────────────
    if line.strip() == "---":
        pdf.ln(2)
        pdf.set_draw_color(180, 180, 180)
        pdf.line(LEFT_MARGIN, pdf.get_y(), pdf.w - RIGHT_MARGIN, pdf.get_y())
        pdf.ln(3)
        continue

    # ── empty line ──────────────────────────────────────────
    if not line.strip():
        pdf.ln(3)
        continue

    # ── bullet ──────────────────────────────────────────────
    if line.startswith("- ") or line.startswith("* "):
        pdf.set_font("Sans", "", 9)
        pdf.set_text_color(30, 30, 30)
        indent = 6
        bullet_w = 5
        text = text_plain(line[2:])
        pdf.set_x(LEFT_MARGIN + indent)
        pdf.cell(bullet_w, 5, "•")
        avail = pdf.epw - indent - bullet_w
        pdf.multi_cell(avail, 5, text)
        continue

    # ── normal paragraph ────────────────────────────────────
    pdf.set_font("Sans", "", 9)
    pdf.set_text_color(30, 30, 30)
    pdf.set_x(LEFT_MARGIN)
    pdf.multi_cell(pdf.epw, 5, text_plain(line))

# flush remaining table
if in_table:
    flush_table(pdf, table_rows, col_chars)

pdf.output(str(OUT))
print(f"PDF gespeichert: {OUT}")
