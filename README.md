# ROS Data Processing and Storage

This project consists of two Python scripts designed to process ROS `.bag` files and associated image data. The scripts extract, analyze, visualize, and store information into a MongoDB database for further use and visualization.

---

## 📂 Project Structure

```
.
├── main.py              # Trajectory and topic extraction from ROS bag files
├── ImageExtractor.py    # Image extraction and storage from CSVs
```

---

## 🔧 Features

### `main.py`
- Reads `.bag` files using `bagpy`
- Extracts trajectory data from ROS topics
- Computes basic statistics (min, max, mean, std)
- Displays histograms for numeric columns
- Plots XY trajectories
- Stores both trajectory and topic data into MongoDB in collections:
  - `ros_data_D.trajectories`, `ros_data_D.topics`
  - `ros_data_H.trajectories`, `ros_data_H.topics`

### `ImageExtractor.py`
- Parses CSV files containing compressed image data
- Extracts and saves decoded images locally
- Uploads raw image bytes to **GridFS** in MongoDB
- Inserts image metadata as **time-series documents** in `image_timeseries` collection

---

## ⚙️ Requirements

- Python 3.x
- MongoDB (local or remote)
- Dependencies:
  - `bagpy`
  - `pandas`
  - `matplotlib`
  - `pymongo`
  - `gridfs`
  - `opencv-python`
  - `numpy`

Install dependencies:
```bash
pip install -r requirements.txt
```

---

## 🚀 How to Run

### 1. Process ROS `.bag` files

Update paths inside `main.py` if needed:

```bash
python main.py
```

This script will:
- Extract trajectory and topic data
- Display stats and plots
- Store the results in MongoDB under `ros_data_D` and `ros_data_H`

---

### 2. Extract and store images from CSV

Update paths inside `ImageExtractor.py` if needed:

```bash
python ImageExtractor.py
```

This script will:
- Decode and save images locally in `extracted_images_D/` and `extracted_images_H/`
- Store image binaries in MongoDB using GridFS
- Create a time-series collection with image metadata and timestamps

---

## 🧠 Notes

- CSV files should include a `data` column with raw compressed image byte arrays
- MongoDB must be running locally at `mongodb://localhost:27017/`
- Project assumes two datasets labeled `D` and `H`

---
