from dash import html, dcc

cycle_gantt_tab = html.Div(
    [
        html.H3("Cycle Gantt Timeline"),
        dcc.Graph(id="cycle-gantt-chart"),
    ]
)