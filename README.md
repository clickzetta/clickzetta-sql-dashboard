# Clickzetta SQL Dashboard

# Getting Started

```
# setup python env, 3.9 tested
pip install -r requirements.txt

# a little bit hack here, will be removed soon
pip install SQLAlchemy==2.0.27
pip install pyarrow==11.0.0

# prepare clickzetta lakehouse connection
mkdir .streamlit
cd .streamlit
echo '[connections.WORKSPACE]' >> secrets.toml
echo 'url = "clickzetta://USER:PASSWORD@INSTANCE.ap-beijing-tencentcloud.api.clickzetta.com/WORKSPACE?virtualcluster=VCLUSTER"' >> secrets.toml

# run the app
streamlit run main.py
```

Open [http://localhost:8501](http://localhost:8501) in your browser.
