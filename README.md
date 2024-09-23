# Clickzetta SQL Dashboard

# Getting Started

```
# setup python env, 3.9 tested
pip install -r requirements.txt

# prepare clickzetta lakehouse connection
mkdir .streamlit
cd .streamlit
echo '[connections.WORKSPACE]' >> secrets.toml
echo 'url = "clickzetta://USER:PASSWORD@INSTANCE.ap-beijing-tencentcloud.api.clickzetta.com/WORKSPACE?virtualcluster=VCLUSTER"' >> secrets.toml

# run the app
streamlit run main.py
```

Open [http://localhost:8501](http://localhost:8501) in your browser.
