import pandas as pd
import xlsxwriter


class ExcelWriter:
    def __init__(self, dfs, outfile):
        self.dfs = dfs
        self.outfile = outfile
        self.writer = pd.ExcelWriter(self.outfile, engine="xlsxwriter")

    def write_to_excel(self, number_column=[], date_column=[]):
        for df in self.dfs:
            sheet = df.name if hasattr(df, "name") else "RESLIST"
            self._write_dataframe(df, sheet)
            self._add_formats()
            self._add_table(df, sheet)
            self._format_date(df, date_column, sheet)
            self._format_number(df, number_column, sheet)

    def write_diff_column(self, df):
        max_row, max_col = df.shape
        ws = self.writer.sheets["RESLIST"]

        for row in range(1, max_row + 1):
            ws.write_formula(row, 20, f"=T{row+1}-S{row+1}", self.num_fmt)

    def write_margin_column(self, df):
        max_row, max_col = df.shape
        ws = self.writer.sheets["RESLIST"]

        for row in range(1, max_row + 1):
            ws.write_formula(row, 21, f"=(S{row+1}-R{row+1})/R{row+1}", self.pct_fmt)

    def set_conditional_format(self, df):
        max_row, max_col = df.shape
        ws = self.writer.sheets["RESLIST"]
        ws.conditional_format(
            0,
            20,
            max_row,
            21,
            {"type": "cell", "criteria": "<", "value": 0, "format": self.negative_fmt},
        )
        ws.conditional_format(
            0, 20, max_row, 21, {"type": "errors", "format": self.negative_fmt}
        )

    def _format_date(self, df, columns, sheet):
        ws = self.writer.sheets[sheet]
        max_row, max_col = df.shape

        for col in columns:
            for row in range(1, max_row + 1):
                ws.write(row, col, df.iloc[row - 1, col], self.date_fmt)

    def _format_number(self, df, columns, sheet):
        max_row, max_col = df.shape
        ws = self.writer.sheets[sheet]

        for col in columns:
            for row in range(1, max_row + 1):
                ws.write(row, col, df.iloc[row - 1, col], self.num_fmt)

    def _write_dataframe(self, df, sheet):
        df.to_excel(
            self.writer,
            sheet_name=sheet,
            startrow=1,
            header=False,
            index=False,
        )

    def _add_formats(self):
        wb = self.writer.book
        self.general_fmt = wb.add_format(
            {"font_name": "Arial", "font_size": 8, "valign": "vcenter"}
        )

        self.num_fmt = wb.add_format(
            {
                "num_format": "0.00",
                "font_name": "Arial",
                "font_size": 8,
                "valign": "vcenter",
            }
        )

        self.pct_fmt = wb.add_format(
            {
                "num_format": "0.0%",
                "font_name": "Arial",
                "font_size": 8,
                "valign": "vcenter",
            }
        )

        self.date_fmt = wb.add_format(
            {
                "num_format": "dd/mm/yyyy",
                "font_name": "Arial",
                "font_size": 8,
                "valign": "vcenter",
            }
        )

        self.negative_fmt = wb.add_format(
            {
                "bg_color": "#FFC7CE",
                "font_color": "#9C0006",
                "font_name": "Arial",
                "font_size": 8,
                "valign": "vcenter",
            }
        )

    def _add_table(self, df, sheet):
        max_row, max_col = df.shape
        ws = self.writer.sheets[sheet]

        column_settings = [{"header": column} for column in df.columns]

        ws.add_table(
            0, 0, max_row, max_col - 1, {"columns": column_settings, "style": None}
        )
        ws.set_column(0, max_col - 1, 10, self.general_fmt)
        ws.set_default_row(19)