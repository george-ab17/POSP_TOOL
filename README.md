# POSP Tool

A FastAPI-based web application for checking insurance payouts based on vehicle and policy parameters. This tool helps POSP (Point of Sales Person) agents calculate optimal payouts for various insurance scenarios.

## Features

- Web-based interface for inputting vehicle and policy details
- Calculation of payout recommendations based on RTO codes and parameters
- Support for multiple vehicle categories, types, and insurance options
- FastAPI backend with HTML frontend

## Installation

1. Clone the repository:
   ```bash
   git clone https://github.com/yourusername/posp_tool.git
   cd posp_tool
   ```

2. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```

## Usage

1. Run the application:
   ```bash
   python -m uvicorn app:app --reload --host 127.0.0.1 --port 8000
   ```

2. Open your web browser and navigate to `http://127.0.0.1:8000`

3. Access the entry page and proceed to the form to input parameters for payout calculation.

## API Endpoints

- `GET /`: Entry page
- `GET /form`: Main form page
- `POST /check-payout`: Submit form data for payout calculation

## Project Structure

- `app.py`: Main FastAPI application
- `entry.html`: Entry page HTML
- `index.html`: Main form HTML
- `requirements.txt`: Python dependencies
- `reference/`: Reference data files (Excel spreadsheets)
- `templates/`: Additional templates
- `logo/`: Logo assets

## Dependencies

- FastAPI
- Uvicorn

See `requirements.txt` for full list.

## Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Submit a pull request

## License

This project is licensed under the MIT License - see the LICENSE file for details.