from dash import html, dcc

failure_tab = html.Div(
    [
        html.H3("Failure Path Visualization"),
        dcc.Graph(id="failure-graph"),
        html.H3("Failure Heatmap"),
        dcc.Graph(id="failure-heatmap"),
        html.H3("Anomaly Detection"),
        dcc.Graph(id="anomaly-chart"),
    ]
)