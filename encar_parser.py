"""
Encar.com Parser — production-ready (май 2026)

Эндпоинты:
  Поиск:      /search/car/list/general                          (origin: www.encar.com)
  Справочник: /search/car/list/general?inav  (1 раз при старте, маппинг code→name)
  Данные:     /v1/readside/vehicle/{id}                         (origin: fem.encar.com)
  Уник. опции:/v1/readside/vehicles/car/{vehicleId}/options/choice
  Диагностика:/v1/readside/diagnosis/vehicle/{id}/sellingpoint  (не у всех авто)

Важно: car_id из поиска != vehicleId внутри vehicle-ответа.
       Уникальные опции берутся по vehicleId (поле vehicleId в ответе vehicle).
"""

import asyncio
import aiohttp
import json
import logging
import random
from dataclasses import dataclass, field, asdict
from typing import Optional
from pathlib import Path

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("encar")

SEARCH_URL    = "https://api.encar.com/search/car/list/general"
VEHICLE_URL   = "https://api.encar.com/v1/readside/vehicle/{id}"
CHOICE_URL    = "https://api.encar.com/v1/readside/vehicles/car/{vehicle_id}/options/choice"
SELLINGPT_URL = "https://api.encar.com/v1/readside/diagnosis/vehicle/{id}/sellingpoint"
DETAIL_PAGE   = "https://fem.encar.com/cars/detail/{id}"
PHOTO_BASE    = "https://ci.encar.com"

HEADERS_SEARCH = {
    "accept": "application/json, text/javascript, */*; q=0.01",
    "accept-language": "ko-KR,ko;q=0.9",
    "origin": "https://www.encar.com",
    "referer": "https://www.encar.com/",
    "sec-fetch-dest": "empty",
    "sec-fetch-mode": "cors",
    "sec-fetch-site": "same-site",
    "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/147.0.0.0 Safari/537.36",
}

HEADERS_DETAIL = {
    "accept": "*/*",
    "accept-language": "ko-KR,ko;q=0.9",
    "origin": "https://fem.encar.com",
    "referer": "https://fem.encar.com/",
    "sec-fetch-dest": "empty",
    "sec-fetch-mode": "cors",
    "sec-fetch-site": "same-site",
    "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/147.0.0.0 Safari/537.36",
}


@dataclass
class CarData:
    car_id:           int
    url:              str           = ""
    title:            str           = ""
    manufacturer:     str           = ""
    model:            str           = ""
    grade:            str           = ""
    year:             Optional[int] = None
    mileage:          Optional[int] = None
    price_won:        Optional[int] = None
    fuel:             str           = ""
    transmission:     str           = ""
    color:            str           = ""
    standard_options: list = field(default_factory=list)  # базовые опции комплектации
    unique_options:   list = field(default_factory=list)  # уникальные опции с ценами
    photos:           list = field(default_factory=list)

    def to_dict(self):
        return asdict(self)


def parse_vehicle(data: dict, car: CarData, option_map: dict) -> Optional[int]:
    """
    Заполняет поля CarData из /v1/readside/vehicle/{id}.
    Возвращает vehicleId (может отличаться от car_id) для запроса choice-опций.
    """
    cat = data.get("category") or {}
    manufacturer = cat.get("manufacturerName") or ""
    model        = cat.get("modelName") or cat.get("modelGroupName") or ""
    grade        = cat.get("gradeName")       or ""
    grade_detail = cat.get("gradeDetailName") or ""

    car.manufacturer = manufacturer
    car.model        = model
    car.grade        = " ".join(filter(None, [grade, grade_detail]))
    car.title        = " ".join(filter(None, [manufacturer, model, car.grade]))
    car.year         = cat.get("formYear")

    adv = data.get("advertisement") or {}
    car.price_won = int(adv.get("price") or 0) * 10_000

    spec = data.get("spec") or {}
    car.mileage      = spec.get("mileage")
    car.fuel         = spec.get("fuelName")         or ""
    car.transmission = spec.get("transmissionName") or ""
    car.color        = spec.get("colorName")        or ""

    # Стандартные опции комплектации — декодируем через справочник
    opts = data.get("options") or {}
    car.standard_options = [
        option_map[c] for c in (opts.get("standard") or []) if c in option_map
    ]

    car.photos = [
        PHOTO_BASE + p["path"]
        for p in (data.get("photos") or [])
        if p.get("path")
    ]

    # vehicleId может отличаться от car_id (дублирующиеся объявления)
    return data.get("vehicleId")


def parse_choice(items: list, car: CarData):
    """
    Уникальные опции с ценами из /vehicles/car/{vehicleId}/options/choice.
    Формат: "Название — X만원"
    """
    result = []
    for item in items:
        name  = item.get("optionName") or ""
        price = item.get("price")       # в 만원
        if name:
            if price:
                result.append(f"{name} — {int(price)}만원")
            else:
                result.append(name)
    car.unique_options = result


class EncarParser:
    def __init__(
        self,
        concurrency: int   = 5,
        delay_min:   float = 0.5,
        delay_max:   float = 1.5,
        output_dir:  str   = "output",
    ):
        self.concurrency = concurrency
        self.delay_min   = delay_min
        self.delay_max   = delay_max
        self.output_dir  = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self._session:    Optional[aiohttp.ClientSession] = None
        self._option_map: dict = {}

    async def _get_session(self) -> aiohttp.ClientSession:
        if not self._session or self._session.closed:
            self._session = aiohttp.ClientSession(
                timeout=aiohttp.ClientTimeout(total=20),
                connector=aiohttp.TCPConnector(limit=self.concurrency),
            )
        return self._session

    async def _get(self, url: str, headers: dict, retries: int = 3):
        session = await self._get_session()
        for attempt in range(1, retries + 1):
            try:
                async with session.get(url, headers=headers) as r:
                    if r.status == 200:
                        return await r.json(content_type=None)
                    elif r.status == 429:
                        wait = 10 * attempt
                        log.warning(f"Rate limit (429), жду {wait}с...")
                        await asyncio.sleep(wait)
                    elif r.status in (400, 401, 403, 404):
                        log.debug(f"HTTP {r.status}: {url}")
                        return None
                    else:
                        await asyncio.sleep(2 * attempt)
            except Exception as e:
                if attempt == retries:
                    log.warning(f"Ошибка: {e}")
                await asyncio.sleep(2 * attempt)
        return None

    async def load_option_map(self) -> dict:
        """Загружаем справочник code→name из iNav один раз."""
        url = (
            f"{SEARCH_URL}"
            f"?count=true"
            f"&q=(And.Hidden.N._.CarType.A.)"
            f"&inav=%7CMetadata%7CSort"
        )
        data = await self._get(url, headers=HEADERS_SEARCH)
        mapping = {}
        if not data:
            log.warning("Не удалось загрузить справочник опций")
            return mapping

        nodes = data.get("iNav", {}).get("Nodes", [])
        for node in nodes:
            if node.get("Name") == "Options":
                for facet in node.get("Facets", []):
                    name  = facet.get("DisplayValue") or ""
                    codes = facet.get("Metadata", {}).get("Code") or []
                    for code in codes:
                        mapping[str(code)] = name
                break

        log.info(f"Справочник опций: {len(mapping)} позиций")
        return mapping

    async def fetch_ids(self, total: int) -> list[int]:
        ids       = []
        offset    = 0
        page_size = 20
        log.info(f"Сбор ID. Цель: {total}")

        while len(ids) < total:
            url = (
                f"{SEARCH_URL}"
                f"?count=true"
                f"&q=(And.Hidden.N._.CarType.A.)"
                f"&sr=%7CModifiedDate%7C{offset}%7C{page_size}"
            )
            data = await self._get(url, headers=HEADERS_SEARCH)
            if not data:
                log.warning("Пустой ответ от поиска — стоп.")
                break

            results = data.get("SearchResults") or []
            if not results:
                log.info("Больше результатов нет.")
                break

            for item in results:
                cid = item.get("Id")
                if cid:
                    ids.append(int(cid))

            log.info(f"  ID собрано: {len(ids)}/{total}  (offset={offset})")

            if len(results) < page_size:
                break

            offset += page_size
            await asyncio.sleep(random.uniform(self.delay_min, self.delay_max))

        return ids[:total]

    async def fetch_car(self, car_id: int) -> Optional[CarData]:
        car = CarData(car_id=car_id, url=DETAIL_PAGE.format(id=car_id))

        # 1. Основные данные + фото + стандартные опции
        vehicle = await self._get(VEHICLE_URL.format(id=car_id), headers=HEADERS_DETAIL)
        if not vehicle:
            return None
        vehicle_id = parse_vehicle(vehicle, car, self._option_map)

        # 2. Уникальные опции с ценами (по vehicleId из ответа)
        vid = vehicle_id or car_id
        choice = await self._get(CHOICE_URL.format(vehicle_id=vid), headers=HEADERS_DETAIL)
        if choice and isinstance(choice, list):
            parse_choice(choice, car)

        await asyncio.sleep(random.uniform(self.delay_min, self.delay_max))
        return car

    async def fetch_all(self, total: int = 100) -> list[CarData]:
        self._option_map = await self.load_option_map()

        car_ids = await self.fetch_ids(total=total)
        log.info(f"Получено {len(car_ids)} ID. Собираем детали...")

        sem     = asyncio.Semaphore(self.concurrency)
        results = []
        errors  = 0

        async def one(car_id: int, idx: int):
            nonlocal errors
            async with sem:
                car = await self.fetch_car(car_id)
                if car:
                    results.append(car)
                    if idx % 25 == 0 or idx == len(car_ids):
                        log.info(f"  Прогресс: {idx}/{len(car_ids)}")
                else:
                    errors += 1

        await asyncio.gather(*[one(cid, i) for i, cid in enumerate(car_ids, 1)])
        log.info(f"Готово. Собрано: {len(results)}, ошибок: {errors}")
        return results

    def save_json(self, cars: list[CarData], filename: str = "encar_cars.json"):
        path = self.output_dir / filename
        with open(path, "w", encoding="utf-8") as f:
            json.dump([c.to_dict() for c in cars], f, ensure_ascii=False, indent=2)
        log.info(f"JSON → {path}  ({len(cars)} авто)")

    def save_csv(self, cars: list[CarData], filename: str = "encar_cars.csv"):
        import csv
        path = self.output_dir / filename
        if not cars:
            return
        fieldnames = [
            "car_id", "url", "title", "manufacturer", "model", "grade",
            "year", "mileage", "price_won", "fuel", "transmission", "color",
            "standard_options", "unique_options", "photos",
        ]
        with open(path, "w", newline="", encoding="utf-8-sig") as f:
            w = csv.DictWriter(f, fieldnames=fieldnames)
            w.writeheader()
            for car in cars:
                row = car.to_dict()
                row["standard_options"] = " | ".join(row["standard_options"])
                row["unique_options"]   = " | ".join(row["unique_options"])
                row["photos"]           = " | ".join(row["photos"])
                w.writerow(row)
        log.info(f"CSV  → {path}  ({len(cars)} авто)")

    async def close(self):
        if self._session and not self._session.closed:
            await self._session.close()


async def main(total: int = 100, output_dir: str = "output"):
    parser = EncarParser(concurrency=5, delay_min=0.5, delay_max=1.2, output_dir=output_dir)
    try:
        cars = await parser.fetch_all(total=total)
        if cars:
            parser.save_json(cars)
            parser.save_csv(cars)
            s = cars[0]
            print(f"\n─── Пример ───")
            print(f"Авто:             {s.title}")
            print(f"Год:              {s.year}")
            print(f"Пробег:           {s.mileage:,} км" if s.mileage else "Пробег: —")
            print(f"Цена:             {s.price_won:,} вон" if s.price_won else "Цена: —")
            print(f"Базовые опции:    {len(s.standard_options)} шт")
            print(f"Уникальные опции: {', '.join(s.unique_options) or '—'}")
            print(f"Фото:             {len(s.photos)} шт")
        else:
            log.warning("Данные не получены.")
    finally:
        await parser.close()


if __name__ == "__main__":
    import sys
    total = int(sys.argv[1]) if len(sys.argv) > 1 else 100
    asyncio.run(main(total=total))
