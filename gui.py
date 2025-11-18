import flet as ft

from database import Database

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
        content = ft.Column(controls=[ft.Text("Actual ranking")], horizontal_alignment=ft.CrossAxisAlignment.CENTER)

        ranking = self.database.get_ranked_players(1, 10)

        table = ft.DataTable(
            columns=[
                ft.DataColumn(ft.Text("Rank")),
                ft.DataColumn(ft.Text("Name")),
                ft.DataColumn(ft.Text("Country")),
                ft.DataColumn(ft.Text("Points"), numeric=True),
            ],
        )

        for player in ranking:
            table.rows.append(ft.DataRow(
                    cells=[
                        ft.DataCell(ft.Text(player[0])),
                        ft.DataCell(ft.Text(player[1])),
                        ft.DataCell(ft.Text(player[2])),
                        ft.DataCell(ft.Text(player[3])),
                    ],
                ))

        content.controls.append(table)

        out = ft.Container(
            content=content,
            expand=True,
        )
        return out

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