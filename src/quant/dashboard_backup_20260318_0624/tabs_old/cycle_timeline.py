from dash import html, dash_table

cycle_timeline_tab = html.Div(
    [
        html.H3("Cycle Timeline"),
        dash_table.DataTable(
            id="cycle-timeline-table",
            columns=[
                {"name": "Step", "id": "step"},
                {"name": "Start", "id": "start"},
                {"name": "End", "id": "end"},
                {"name": "Duration (s)", "id": "duration"},
                {"name": "Status", "id": "status"},
            ],
            style_table={"overflowX": "auto"},
            style_cell={"padding": "6px", "fontFamily": "Arial"},
            style_header={"fontWeight": "bold"},
        ),
    ]
)