from quant.dashboard.app import create_app

def main():
    app = create_app()
    app.run_server(host="0.0.0.0", port=8050)

if __name__ == "__main__":
    main()