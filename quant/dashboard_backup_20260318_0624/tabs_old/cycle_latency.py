from dash import html, dcc

cycle_latency_tab = html.Div(
    [
        html.H3("Cycle Latency Analytics"),
        dcc.Graph(id="cycle-latency-chart"),
        dcc.Graph(id="step-latency-chart"),
    ]
)