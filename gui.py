import flet as ft

from database import Database


def get_superscript_text(text_to_convert):
    superscript_map = str.maketrans("0123456789+-", "⁰¹²³⁴⁵⁶⁷⁸⁹⁺⁻")
    return str(text_to_convert).translate(superscript_map)

class GUI():
    def __init__(self, page):
            self.page = page
            self.appbar = ft.AppBar(
                leading=ft.Icon(name=ft.Icons.SPORTS_BASEBALL_ROUNDED),
                title=ft.Text("TennisMath"),
                actions=[
                    ft.TextButton("Home"),
                ],
            )
            self.database = Database("tennis.db")

    def buit_home(self):
        ranking_column = ft.Column(controls=[ft.Text("Actual ranking", size=20, weight=ft.FontWeight.BOLD,)], horizontal_alignment=ft.CrossAxisAlignment.CENTER)

        ranking = self.database.get_ranked_players(1, 20)

        table = ft.DataTable(
            columns=[
                ft.DataColumn(ft.Text("Rank")),
                ft.DataColumn(ft.Text("Name")),
                ft.DataColumn(ft.Text("Country")),
                ft.DataColumn(ft.Text("Points"), numeric=True),
            ],
            column_spacing=12,
            #data_row_max_height=40,
            heading_row_height=40,
            heading_text_style=ft.TextStyle(size=14, weight=ft.FontWeight.BOLD),
            #data_text_style=ft.TextStyle(size=12)#, weight=ft.FontWeight.BOLD)
        )

        scrooling_table = ft.Column(horizontal_alignment=ft.CrossAxisAlignment.CENTER, scroll=ft.ScrollMode.AUTO, height=500)

        for player in ranking:
            last_rank, last_points = self.database.get_player_history_at_date(player[5], 1)
            rank_dif = player[0] - last_rank
            points_dif = player[3] - last_points
            rank_dif_view = ft.TextSpan("")
            points_dif_view = ft.Text("-")
            if rank_dif > 0:
                rank_dif_view = ft.TextSpan(get_superscript_text(f"+{rank_dif}"), ft.TextStyle(color="green"))
            elif rank_dif < 0:
                rank_dif_view = ft.TextSpan(get_superscript_text(f"{rank_dif}"), ft.TextStyle(color="red"))
            if points_dif > 0:
                points_dif_view = ft.Text(f"+{points_dif}", color="green")
            elif points_dif < 0:
                points_dif_view = ft.Text(f"{points_dif}", color="red")
            table.rows.append(ft.DataRow(
                    cells=[
                        ft.DataCell(ft.Text(player[0], spans=[rank_dif_view])),
                        ft.DataCell(ft.Text(player[1])),
                        ft.DataCell(ft.Text(player[2])),
                        ft.DataCell(ft.Column([ft.Text(player[3]), points_dif_view],
                                              alignment=ft.MainAxisAlignment.CENTER,
                                              horizontal_alignment=ft.CrossAxisAlignment.CENTER,
                                              spacing=2)),
                    ],
                ))

        scrooling_table.controls.append(table)
        ranking_column.controls.append(scrooling_table)

        out = ft.Container(
            content=ranking_column,
            padding=20,
            expand=True,
        )
        return ranking_column

    def route_change(self, e):
        if self.page.route == "/":
            self.page.clean()
            self.page.add(self.buit_home())
        self.page.update()

def main(page: ft.Page):
    gui = GUI(page)

    page.horizontal_alignment = "center"
    page.scroll = ft.ScrollMode.AUTO
    page.appbar = gui.appbar
    page.on_route_change = gui.route_change
    #page.on_connect = gui.route_change(None)

    page.go("/")

    page.update()