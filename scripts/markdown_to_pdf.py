#!/usr/bin/env python3
"""Convert the repository's Markdown reading material into mobile-friendly PDFs."""

from __future__ import annotations

import argparse
import html
import re
from pathlib import Path

from reportlab.lib import colors
from reportlab.lib.enums import TA_CENTER, TA_LEFT
from reportlab.lib.pagesizes import A4
from reportlab.lib.styles import ParagraphStyle, getSampleStyleSheet
from reportlab.lib.units import mm
from reportlab.pdfbase import pdfmetrics
from reportlab.pdfbase.ttfonts import TTFont
from reportlab.platypus import (
    BaseDocTemplate,
    Frame,
    HRFlowable,
    PageTemplate,
    Paragraph,
    Preformatted,
    Spacer,
    Table,
    TableStyle,
)


PAGE_WIDTH, PAGE_HEIGHT = A4
LEFT_MARGIN = 15 * mm
RIGHT_MARGIN = 15 * mm
TOP_MARGIN = 18 * mm
BOTTOM_MARGIN = 17 * mm
CONTENT_WIDTH = PAGE_WIDTH - LEFT_MARGIN - RIGHT_MARGIN

REGULAR_FONT = "/System/Library/Fonts/Supplemental/Arial Unicode.ttf"
BOLD_FONT = "/System/Library/Fonts/STHeiti Medium.ttc"


def register_fonts() -> None:
    pdfmetrics.registerFont(TTFont("ReaderRegular", REGULAR_FONT))
    pdfmetrics.registerFont(TTFont("ReaderBold", BOLD_FONT, subfontIndex=0))
    pdfmetrics.registerFontFamily(
        "ReaderRegular",
        normal="ReaderRegular",
        bold="ReaderBold",
        italic="ReaderRegular",
        boldItalic="ReaderBold",
    )


def clean_text(value: str) -> str:
    replacements = {
        "\u2011": "-",
        "\ufe0f": "",
        "✅": "是",
        "❌": "否",
        "⚠": "注意",
        "⭐": "★",
        "⏱": "时间",
        "⏭": "下一步",
        "❓": "?",
        "➖": "-",
        "➡": "->",
        "⬅": "<-",
    }
    for source, target in replacements.items():
        value = value.replace(source, target)
    # Decorative emoji are not reliably supported by PDF TrueType embedding.
    value = re.sub(r"[\U00010000-\U0010FFFF]", "", value)
    value = value.replace("\u200b", "").replace("\ufeff", "")
    return value


def inline_markup(value: str) -> str:
    value = clean_text(value)
    value = re.sub(r"<br\s*/?>", "\u0000BR\u0000", value, flags=re.IGNORECASE)
    value = re.sub(r"</?strong>", "**", value, flags=re.IGNORECASE)
    value = re.sub(r"</?em>", "*", value, flags=re.IGNORECASE)
    value = re.sub(r"</?(?:p|sub)(?:\s+[^>]*)?>", "", value, flags=re.IGNORECASE)
    value = html.escape(value, quote=True).replace("\x00BR\x00", "<br/>")
    admonitions = {
        "[!NOTE]": "说明",
        "[!TIP]": "提示",
        "[!IMPORTANT]": "重要",
        "[!WARNING]": "警告",
        "[!CAUTION]": "注意",
    }
    for marker, label in admonitions.items():
        value = value.replace(marker, f"<b>{label}</b>")
    value = re.sub(
        r"!\[([^]]*)\]\([^)]+\)",
        lambda m: f"<i>[图片: {m.group(1) or '未命名'}]</i>",
        value,
    )
    def render_link(match: re.Match[str]) -> str:
        label, target = match.group(1), match.group(2)
        if re.match(r"^(?:https?://|mailto:)", target, flags=re.IGNORECASE):
            return f'<a href="{target}" color="#2563EB">{label}</a>'
        return label

    value = re.sub(r"\[([^]]+)\]\(([^)]+)\)", render_link, value)
    value = re.sub(r"`([^`]+)`", r'<font name="ReaderBold" color="#334155">\1</font>', value)
    value = re.sub(r"\*\*([^*]+)\*\*", r"<b>\1</b>", value)
    value = re.sub(r"__([^_]+)__", r"<b>\1</b>", value)
    value = re.sub(r"~~([^~]+)~~", r"<strike>\1</strike>", value)
    value = re.sub(r"(?<!\*)\*([^*]+)\*(?!\*)", r"<i>\1</i>", value)
    return value


def paragraph_style(name: str, **kwargs) -> ParagraphStyle:
    defaults = dict(
        fontName="ReaderRegular",
        fontSize=10.2,
        leading=16.2,
        textColor=colors.HexColor("#1F2937"),
        wordWrap="CJK",
        allowWidows=0,
        allowOrphans=0,
    )
    defaults.update(kwargs)
    return ParagraphStyle(name, **defaults)


def build_styles() -> dict[str, ParagraphStyle]:
    getSampleStyleSheet()  # Initialize ReportLab's standard style registry.
    styles = {
        "body": paragraph_style("Body", spaceAfter=5.5),
        "quote": paragraph_style(
            "Quote",
            fontSize=9.5,
            leading=15,
            leftIndent=9,
            rightIndent=7,
            textColor=colors.HexColor("#475569"),
            borderColor=colors.HexColor("#60A5FA"),
            borderWidth=1.8,
            borderPadding=(6, 7, 6, 9),
            backColor=colors.HexColor("#EFF6FF"),
            spaceBefore=4,
            spaceAfter=8,
        ),
        "code": paragraph_style(
            "Code",
            fontSize=7.6,
            leading=11.2,
            leftIndent=6,
            rightIndent=6,
            borderColor=colors.HexColor("#CBD5E1"),
            borderWidth=0.5,
            borderPadding=7,
            backColor=colors.HexColor("#F8FAFC"),
            spaceBefore=4,
            spaceAfter=8,
        ),
        "math": paragraph_style(
            "Math",
            fontName="ReaderBold",
            alignment=TA_CENTER,
            fontSize=9.5,
            leading=15,
            textColor=colors.HexColor("#334155"),
            backColor=colors.HexColor("#F8FAFC"),
            borderPadding=6,
            spaceBefore=4,
            spaceAfter=8,
        ),
    }
    heading_specs = {
        1: (22, 29, "#0F3B66", 16, 12),
        2: (16, 22, "#155E75", 14, 8),
        3: (13, 19, "#1D4ED8", 11, 6),
        4: (11.5, 17, "#334155", 9, 4),
        5: (10.5, 16, "#475569", 7, 3),
        6: (10, 15, "#64748B", 6, 3),
    }
    for level, (size, leading, color, before, after) in heading_specs.items():
        styles[f"h{level}"] = paragraph_style(
            f"Heading{level}",
            fontName="ReaderBold",
            fontSize=size,
            leading=leading,
            textColor=colors.HexColor(color),
            spaceBefore=before,
            spaceAfter=after,
            keepWithNext=True,
        )
    return styles


def is_table_separator(line: str) -> bool:
    cells = split_table_row(line)
    return bool(cells) and all(re.fullmatch(r":?-{3,}:?", cell.strip()) for cell in cells)


def split_table_row(line: str) -> list[str]:
    line = line.strip()
    if line.startswith("|"):
        line = line[1:]
    if line.endswith("|"):
        line = line[:-1]
    return [cell.strip() for cell in re.split(r"(?<!\\)\|", line)]


def table_flowable(rows: list[list[str]], styles: dict[str, ParagraphStyle]) -> Table:
    column_count = max(len(row) for row in rows)
    normalized = [row + [""] * (column_count - len(row)) for row in rows]
    if column_count <= 3:
        font_size = 8.3
    elif column_count <= 5:
        font_size = 7.3
    elif column_count <= 7:
        font_size = 6.5
    else:
        font_size = 5.7
    cell_style = paragraph_style(
        f"TableCell{column_count}",
        fontSize=font_size,
        leading=font_size * 1.45,
        spaceAfter=0,
    )
    header_style = paragraph_style(
        f"TableHeader{column_count}",
        fontName="ReaderBold",
        fontSize=font_size,
        leading=font_size * 1.45,
        textColor=colors.white,
        spaceAfter=0,
    )
    data = []
    for row_index, row in enumerate(normalized):
        style = header_style if row_index == 0 else cell_style
        data.append([Paragraph(inline_markup(cell), style) for cell in row])

    lengths = []
    for column in range(column_count):
        longest = max(len(clean_text(row[column])) for row in normalized)
        lengths.append(max(5.0, min(24.0, longest ** 0.72)))
    total = sum(lengths)
    widths = [CONTENT_WIDTH * length / total for length in lengths]
    table = Table(data, colWidths=widths, repeatRows=1, hAlign="LEFT", splitByRow=1)
    table.setStyle(
        TableStyle(
            [
                ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#0F4C75")),
                ("VALIGN", (0, 0), (-1, -1), "TOP"),
                ("GRID", (0, 0), (-1, -1), 0.35, colors.HexColor("#CBD5E1")),
                ("LEFTPADDING", (0, 0), (-1, -1), 4),
                ("RIGHTPADDING", (0, 0), (-1, -1), 4),
                ("TOPPADDING", (0, 0), (-1, -1), 4),
                ("BOTTOMPADDING", (0, 0), (-1, -1), 4),
                ("ROWBACKGROUNDS", (0, 1), (-1, -1), [colors.white, colors.HexColor("#F8FAFC")]),
            ]
        )
    )
    return table


def is_special_start(lines: list[str], index: int) -> bool:
    stripped = lines[index].strip()
    if not stripped:
        return True
    if stripped.startswith(("#", ">", "```", "~~~")):
        return True
    if re.match(r"^([-*_])(?:\s*\1){2,}\s*$", stripped):
        return True
    if re.match(r"^\s*(?:[-+*]|\d+[.)])\s+", lines[index]):
        return True
    if stripped.startswith("$$"):
        return True
    if index + 1 < len(lines) and "|" in stripped and is_table_separator(lines[index + 1]):
        return True
    return False


def wrap_code_line(line: str, width: int = 92) -> list[str]:
    line = line.expandtabs(4)
    if len(line) <= width:
        return [line]
    indent = re.match(r"\s*", line).group(0)
    chunks = []
    remaining = line
    while len(remaining) > width:
        cut = remaining.rfind(" ", 0, width)
        if cut <= len(indent):
            cut = width
        chunks.append(remaining[:cut])
        remaining = indent + "  " + remaining[cut:].lstrip()
    chunks.append(remaining)
    return chunks


def markdown_story(text: str, styles: dict[str, ParagraphStyle]) -> list:
    lines = text.splitlines()
    story = []
    index = 0
    while index < len(lines):
        raw = lines[index]
        stripped = raw.strip()
        if not stripped:
            index += 1
            continue

        fence_match = re.match(r"^\s*(```|~~~)", raw)
        if fence_match:
            fence = fence_match.group(1)
            index += 1
            code_lines = []
            while index < len(lines) and not lines[index].lstrip().startswith(fence):
                code_lines.extend(wrap_code_line(clean_text(lines[index])))
                index += 1
            if index < len(lines):
                index += 1
            code = "\n".join(code_lines) or " "
            story.append(Preformatted(code, styles["code"], maxLineLength=96))
            continue

        heading_match = re.match(r"^(#{1,6})\s+(.+?)\s*#*\s*$", stripped)
        if heading_match:
            level = len(heading_match.group(1))
            story.append(Paragraph(inline_markup(heading_match.group(2)), styles[f"h{level}"]))
            index += 1
            continue

        if index + 1 < len(lines) and "|" in stripped and is_table_separator(lines[index + 1]):
            rows = [split_table_row(raw)]
            index += 2
            while index < len(lines) and "|" in lines[index] and lines[index].strip():
                rows.append(split_table_row(lines[index]))
                index += 1
            story.extend([table_flowable(rows, styles), Spacer(1, 8)])
            continue

        if stripped.startswith(">"):
            quote_lines = []
            while index < len(lines) and lines[index].strip().startswith(">"):
                quote_lines.append(re.sub(r"^\s*>\s?", "", lines[index]))
                index += 1
            content = "<br/>".join(inline_markup(line) if line else " " for line in quote_lines)
            story.append(Paragraph(content, styles["quote"]))
            continue

        if stripped.startswith("$$"):
            math_lines = [stripped]
            index += 1
            if stripped == "$$":
                while index < len(lines):
                    math_lines.append(lines[index].strip())
                    index += 1
                    if math_lines[-1].endswith("$$"):
                        break
            expression = " ".join(math_lines).strip("$")
            story.append(Paragraph(inline_markup(expression), styles["math"]))
            continue

        if re.match(r"^([-*_])(?:\s*\1){2,}\s*$", stripped):
            story.append(
                HRFlowable(
                    width="100%",
                    thickness=0.7,
                    color=colors.HexColor("#CBD5E1"),
                    spaceBefore=6,
                    spaceAfter=8,
                )
            )
            index += 1
            continue

        list_match = re.match(r"^(\s*)([-+*]|\d+[.)])\s+(.+)$", raw)
        if list_match:
            indent_level = min(4, len(list_match.group(1).replace("\t", "    ")) // 2)
            marker = list_match.group(2)
            bullet = "•" if marker in {"-", "+", "*"} else marker.rstrip(").") + "."
            list_style = paragraph_style(
                f"List{indent_level}",
                fontSize=10,
                leading=15.5,
                leftIndent=17 + indent_level * 12,
                firstLineIndent=0,
                bulletIndent=4 + indent_level * 12,
                spaceAfter=3,
            )
            story.append(Paragraph(inline_markup(list_match.group(3)), list_style, bulletText=bullet))
            index += 1
            continue

        paragraph_lines = [stripped]
        index += 1
        while index < len(lines) and not is_special_start(lines, index):
            paragraph_lines.append(lines[index].strip())
            index += 1
        story.append(Paragraph(inline_markup(" ".join(paragraph_lines)), styles["body"]))
    return story


class ReaderDocTemplate(BaseDocTemplate):
    def __init__(self, filename: str, source_label: str, **kwargs):
        self.source_label = clean_text(source_label)
        super().__init__(filename, **kwargs)
        frame = Frame(
            LEFT_MARGIN,
            BOTTOM_MARGIN,
            CONTENT_WIDTH,
            PAGE_HEIGHT - TOP_MARGIN - BOTTOM_MARGIN,
            id="reader-frame",
            leftPadding=0,
            rightPadding=0,
            topPadding=0,
            bottomPadding=0,
        )
        self.addPageTemplates(PageTemplate(id="reader", frames=[frame], onPage=self.draw_page))

    def draw_page(self, canvas, doc) -> None:
        canvas.saveState()
        canvas.setStrokeColor(colors.HexColor("#E2E8F0"))
        canvas.setLineWidth(0.4)
        canvas.line(LEFT_MARGIN, PAGE_HEIGHT - 12 * mm, PAGE_WIDTH - RIGHT_MARGIN, PAGE_HEIGHT - 12 * mm)
        canvas.setFont("ReaderRegular", 7.2)
        canvas.setFillColor(colors.HexColor("#64748B"))
        label = self.source_label
        if len(label) > 58:
            label = "..." + label[-55:]
        canvas.drawString(LEFT_MARGIN, PAGE_HEIGHT - 9.5 * mm, label)
        canvas.drawRightString(PAGE_WIDTH - RIGHT_MARGIN, 8.5 * mm, f"第 {doc.page} 页")
        canvas.restoreState()


def convert_file(source: Path, destination: Path, root: Path, styles: dict[str, ParagraphStyle]) -> None:
    destination.parent.mkdir(parents=True, exist_ok=True)
    relative = source.relative_to(root).as_posix()
    doc = ReaderDocTemplate(
        str(destination),
        source_label=relative,
        pagesize=A4,
        leftMargin=LEFT_MARGIN,
        rightMargin=RIGHT_MARGIN,
        topMargin=TOP_MARGIN,
        bottomMargin=BOTTOM_MARGIN,
        title=source.stem,
        author="Stock Evaluation Learning Notes",
        subject=relative,
    )
    story = markdown_story(source.read_text(encoding="utf-8"), styles)
    doc.build(story)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--root", type=Path, default=Path.cwd())
    parser.add_argument("--output", type=Path, default=Path("output/pdf"))
    args = parser.parse_args()
    root = args.root.resolve()
    output = args.output if args.output.is_absolute() else root / args.output
    register_fonts()
    styles = build_styles()
    sources = sorted(path for path in root.rglob("*.md") if output not in path.parents)
    for source in sources:
        destination = output / source.relative_to(root).with_suffix(".pdf")
        convert_file(source, destination, root, styles)
        print(f"{source.relative_to(root)} -> {destination.relative_to(root)}")


if __name__ == "__main__":
    main()
