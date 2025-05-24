from pathlib import Path

import requests

z = requests.get(
    "https://www.certificatiederivati.it/db_bs_scheda_certificato.asp?isin=XS2770641970",
)
Path("prova.html").write_text(z.text)
