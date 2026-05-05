"""
Модуль перевода корейских данных на английский.

Стратегия:
  - Словари: марка, топливо, КПП, цвет (мгновенно, офлайн)
  - LibreTranslate (локальный): опции и названия моделей (без лимитов)
  - Кэш: переведённые строки сохраняются в translation_cache.json

Запуск LibreTranslate:
  pip install libretranslate
  libretranslate --load-only ko,en
  # Сервер будет на http://localhost:5000
"""

import asyncio
import aiohttp
import json
import logging
from pathlib import Path

log = logging.getLogger("encar")

# ── Словари ───────────────────────────────────────────────────────────────────

MANUFACTURER_MAP = {
    "현대": "Hyundai",
    "기아": "Kia",
    "제네시스": "Genesis",
    "쉐보레(GM대우)": "Chevrolet (GM Daewoo)",
    "쉐보레": "Chevrolet",
    "르노코리아(삼성)": "Renault Korea (Samsung)",
    "KG모빌리티(쌍용)": "KG Mobility (Ssangyong)",
    "BMW": "BMW",
    "벤츠": "Mercedes-Benz",
    "아우디": "Audi",
    "볼보": "Volvo",
    "폭스바겐": "Volkswagen",
    "포르쉐": "Porsche",
    "테슬라": "Tesla",
    "렉서스": "Lexus",
    "도요타": "Toyota",
    "혼다": "Honda",
    "닛산": "Nissan",
    "미니": "Mini",
    "랜드로버": "Land Rover",
    "재규어": "Jaguar",
    "포드": "Ford",
    "링컨": "Lincoln",
    "캐딜락": "Cadillac",
    "지프": "Jeep",
    "크라이슬러": "Chrysler",
    "마세라티": "Maserati",
    "페라리": "Ferrari",
    "람보르기니": "Lamborghini",
    "벤틀리": "Bentley",
    "롤스로이스": "Rolls-Royce",
    "맥라렌": "McLaren",
    "푸조": "Peugeot",
    "시트로엥/DS": "Citroen/DS",
    "피아트": "Fiat",
    "알파 로메오": "Alfa Romeo",
    "BYD": "BYD",
    "폴스타": "Polestar",
    "GMC": "GMC",
    "인피니티": "Infiniti",
    "기타 수입차": "Other Imports",
    "기타 제조사": "Other",
}

FUEL_MAP = {
    "가솔린": "Gasoline",
    "디젤": "Diesel",
    "전기": "Electric",
    "가솔린+전기": "Gasoline+Electric (Hybrid)",
    "디젤+전기": "Diesel+Electric (Hybrid)",
    "LPG(일반인 구입)": "LPG",
    "LPG+전기": "LPG+Electric",
    "가솔린+LPG": "Gasoline+LPG",
    "수소": "Hydrogen",
    "가솔린+CNG": "Gasoline+CNG",
    "CNG": "CNG",
    "기타": "Other",
}

TRANSMISSION_MAP = {
    "오토": "Automatic",
    "수동": "Manual",
    "세미오토": "Semi-Automatic",
    "CVT": "CVT",
    "기타": "Other",
}

COLOR_MAP = {
    "흰색": "White",
    "검정색": "Black",
    "은색": "Silver",
    "쥐색": "Gray",
    "청색": "Blue",
    "빨간색": "Red",
    "갈색": "Brown",
    "노란색": "Yellow",
    "녹색": "Green",
    "진주색": "Pearl White",
    "하늘색": "Sky Blue",
    "주황색": "Orange",
    "보라색": "Purple",
    "분홍색": "Pink",
    "연두색": "Light Green",
    "금색": "Gold",
    "담녹색": "Dark Green",
    "자주색": "Maroon",
    "은하색": "Galaxy Silver",
    "연금색": "Light Gold",
    "명은색": "Bright Silver",
    "청옥색": "Teal",
    "갈대색": "Reed Brown",
    "흰색투톤": "White Two-Tone",
    "검정투톤": "Black Two-Tone",
    "은색투톤": "Silver Two-Tone",
    "진주투톤": "Pearl Two-Tone",
    "기타": "Other",
}


def translate_static(value: str, mapping: dict) -> str:
    """Переводим через словарь, если нет — возвращаем оригинал."""
    return mapping.get(value, value)


# ── LibreTranslate ─────────────────────────────────────────────────────────────

LIBRETRANSLATE_URL = "http://localhost:5000/translate"
CACHE_FILE         = Path("translation_cache.json")


class Translator:
    """
    Переводчик через локальный LibreTranslate с персистентным кэшем.

    Установка LibreTranslate:
        pip install libretranslate
        libretranslate --load-only ko,en

    Кэш сохраняется в translation_cache.json — при повторных запусках
    уже переведённые строки не запрашиваются снова.
    """

    def __init__(
        self,
        session:    aiohttp.ClientSession,
        url:        str  = LIBRETRANSLATE_URL,
        cache_file: Path = CACHE_FILE,
    ):
        self._session    = session
        self._url        = url
        self._cache_file = cache_file
        self._cache: dict = self._load_cache()
        self._lock        = asyncio.Lock()
        self._available   = True   # становится False если сервер недоступен

    def _load_cache(self) -> dict:
        if self._cache_file.exists():
            try:
                return json.loads(self._cache_file.read_text(encoding="utf-8"))
            except Exception:
                pass
        return {}

    def _save_cache(self):
        try:
            self._cache_file.write_text(
                json.dumps(self._cache, ensure_ascii=False, indent=2),
                encoding="utf-8",
            )
        except Exception as e:
            log.warning(f"Не удалось сохранить кэш: {e}")

    async def check_available(self) -> bool:
        """Проверяем что LibreTranslate запущен."""
        try:
            async with self._session.get(
                self._url.replace("/translate", "/languages"),
                timeout=aiohttp.ClientTimeout(total=3),
            ) as r:
                self._available = r.status == 200
        except Exception:
            self._available = False

        if not self._available:
            log.warning(
                "LibreTranslate недоступен на %s\n"
                "  Запустите: libretranslate --load-only ko,en\n"
                "  Данные будут сохранены на корейском.",
                self._url,
            )
        else:
            log.info("LibreTranslate подключён: %s", self._url)

        return self._available

    async def translate(self, text: str) -> str:
        """Переводим одну строку ko→en. Результат кэшируется."""
        if not text or not text.strip():
            return text

        # Если текст уже латиница — не переводим
        if all(ord(c) < 128 for c in text.replace(" ", "").replace("-", "")):
            return text

        # Из кэша
        if text in self._cache:
            return self._cache[text]

        # Если сервер недоступен — возвращаем оригинал
        if not self._available:
            return text

        try:
            async with self._session.post(
                self._url,
                json={"q": text, "source": "ko", "target": "en", "format": "text"},
                timeout=aiohttp.ClientTimeout(total=10),
            ) as r:
                if r.status == 200:
                    data  = await r.json(content_type=None)
                    translated = data.get("translatedText") or text
                else:
                    log.debug(f"LibreTranslate HTTP {r.status} для: {text}")
                    return text
        except Exception as e:
            log.debug(f"LibreTranslate ошибка: {e}")
            return text

        async with self._lock:
            self._cache[text] = translated
            self._save_cache()

        return translated

    async def translate_list(self, items: list[str]) -> list[str]:
        """
        Переводим список строк.
        LibreTranslate локальный — задержка не нужна, идём параллельно.
        """
        tasks = [self.translate(item) for item in items]
        return await asyncio.gather(*tasks)
