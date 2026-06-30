from __future__ import annotations

import asyncio
import base64
import binascii
import os

import httpx
from dotenv import load_dotenv


load_dotenv()


class VisionError(Exception):
    """Base error for user-facing AI Vision failures."""


class VisionConfigError(VisionError):
    pass


class VisionImageDownloadError(VisionError):
    pass


class VisionImageTooLargeError(VisionError):
    pass


class VisionGeminiError(VisionError):
    pass


def _env_float(name: str, default: float) -> float:
    raw = os.getenv(name)
    if raw is None or raw.strip() == '':
        return default
    try:
        return float(raw)
    except (TypeError, ValueError):
        return default


def _env_int(name: str, default: int) -> int:
    raw = os.getenv(name)
    if raw is None or raw.strip() == '':
        return int(default)
    try:
        return int(raw)
    except (TypeError, ValueError):
        return int(default)


DEFAULT_VISION_SYSTEM_PROMPT = (
    'Bạn là trợ lý AI Vision trong Discord. '
    'Luôn trả lời bằng tiếng Việt, rõ ràng, ngắn gọn và hữu ích. '
    'Tự điều chỉnh cách xưng hô theo vibe của user: mình/bạn, tôi/bạn, tôi/ông, t/m, hoặc tao/mày. '
    'Chỉ dùng mày/tao khi user đã dùng trước và ngữ cảnh rõ là thân mật, vui vẻ. '
    'Nếu cuộc trò chuyện căng thẳng, nhạy cảm, tranh cãi hoặc có công kích cá nhân, hãy chuyển sang mình/bạn hoặc tôi/bạn. '
    'Có thể nhận diện lời chửi đùa giữa bạn bè, nhưng không được làm căng thêm. '
    'Nếu ảnh có chữ, hãy đọc và tóm tắt nội dung quan trọng. '
    'Nếu không chắc về chi tiết nào, hãy nói là bạn không chắc thay vì đoán quá mức.'
)


class GeminiVisionClient:
    def __init__(
        self,
        *,
        api_key: str | None = None,
        model: str | None = None,
        timeout: float | None = None,
        max_output_tokens: int | None = None,
        temperature: float | None = None,
        system_prompt: str | None = None,
    ):
        self.api_key = api_key or os.getenv('GEMINI_API_KEY')
        self.model = model or os.getenv('GEMINI_VISION_MODEL', 'gemini-2.5-flash-lite')
        self.timeout = timeout if timeout is not None else max(1.0, _env_float('AI_HTTP_TIMEOUT', 60.0))
        self.max_output_tokens = max_output_tokens or max(1, _env_int('AI_VISION_MAX_OUTPUT_TOKENS', 900))
        self.temperature = (
            temperature
            if temperature is not None
            else max(0.0, min(2.0, _env_float('AI_TEMPERATURE', 0.4)))
        )
        self.system_prompt = system_prompt or os.getenv('AI_VISION_SYSTEM_PROMPT') or DEFAULT_VISION_SYSTEM_PROMPT

    @staticmethod
    def _load_gemini_sdk():
        try:
            from google import genai
            from google.genai import types
        except ImportError as e:
            raise VisionConfigError('Thiếu package google-genai. Hãy cài dependencies từ requirements.txt.') from e
        return genai, types

    @staticmethod
    def _parse_data_url(data_url: str) -> tuple[str, bytes]:
        header, separator, payload = str(data_url or '').partition(',')
        if separator != ',' or not header.startswith('data:'):
            raise VisionImageDownloadError('Data URL ảnh không hợp lệ.')

        metadata = header[5:]
        parts = [part.strip() for part in metadata.split(';') if part.strip()]
        content_type = parts[0].lower() if parts else 'application/octet-stream'
        if 'base64' not in {part.lower() for part in parts[1:]}:
            raise VisionImageDownloadError('Data URL ảnh phải dùng base64.')

        try:
            image_bytes = base64.b64decode(payload, validate=True)
        except (binascii.Error, ValueError) as e:
            raise VisionImageDownloadError('Data URL ảnh có base64 không hợp lệ.') from e

        if not image_bytes:
            raise VisionImageDownloadError('Data URL ảnh rỗng.')
        return content_type, image_bytes

    @staticmethod
    def _extract_response_text(response) -> str:
        try:
            text = str(getattr(response, 'text', '') or '').strip()
        except Exception:
            text = ''
        if text:
            return text

        candidates = getattr(response, 'candidates', None) or []
        for candidate in candidates:
            content = getattr(candidate, 'content', None)
            parts = getattr(content, 'parts', None) or []
            joined = '\n'.join(
                str(getattr(part, 'text', '') or '').strip()
                for part in parts
                if str(getattr(part, 'text', '') or '').strip()
            ).strip()
            if joined:
                return joined
        return ''

    def _generate_content_sync(self, *, question: str, content_type: str, image_bytes: bytes) -> str:
        genai, types = self._load_gemini_sdk()
        client = genai.Client(
            api_key=self.api_key,
            http_options=types.HttpOptions(timeout=int(self.timeout * 1000)),
        )
        try:
            response = client.models.generate_content(
                model=self.model,
                contents=[
                    question,
                    types.Part.from_bytes(data=image_bytes, mime_type=content_type),
                ],
                config=types.GenerateContentConfig(
                    system_instruction=self.system_prompt,
                    max_output_tokens=self.max_output_tokens,
                    temperature=self.temperature,
                ),
            )
        except Exception as e:
            raise VisionGeminiError(str(e)) from e
        finally:
            close = getattr(client, 'close', None)
            if callable(close):
                try:
                    close()
                except Exception:
                    pass

        text = self._extract_response_text(response)
        if not text:
            raise VisionGeminiError('Gemini không trả về nội dung văn bản.')
        return text

    async def image_url_to_data_url(
        self,
        *,
        image_url: str,
        content_type: str,
        max_bytes: int,
    ) -> str:
        headers = {'User-Agent': 'study-discord-bot/ai-vision'}
        try:
            async with httpx.AsyncClient(timeout=self.timeout, follow_redirects=True) as client:
                async with client.stream('GET', image_url, headers=headers) as response:
                    if response.status_code >= 400:
                        raise VisionImageDownloadError(f'Discord image URL returned HTTP {response.status_code}.')

                    content_length = response.headers.get('content-length')
                    if content_length and int(content_length) > max_bytes:
                        raise VisionImageTooLargeError('Ảnh vượt quá giới hạn dung lượng.')

                    chunks: list[bytes] = []
                    total = 0
                    async for chunk in response.aiter_bytes():
                        total += len(chunk)
                        if total > max_bytes:
                            raise VisionImageTooLargeError('Ảnh vượt quá giới hạn dung lượng.')
                        chunks.append(chunk)
        except VisionError:
            raise
        except (httpx.TimeoutException, httpx.RequestError, ValueError) as e:
            raise VisionImageDownloadError(str(e)) from e

        if not chunks:
            raise VisionImageDownloadError('Discord image URL returned an empty body.')

        encoded = base64.b64encode(b''.join(chunks)).decode('ascii')
        return f'data:{content_type};base64,{encoded}'

    async def analyze_image_from_url(
        self,
        *,
        question: str,
        image_url: str,
        content_type: str,
        max_bytes: int,
    ) -> str:
        if not self.api_key:
            raise VisionConfigError('Thiếu GEMINI_API_KEY.')

        self._load_gemini_sdk()
        data_url = await self.image_url_to_data_url(
            image_url=image_url,
            content_type=content_type,
            max_bytes=max_bytes,
        )
        return await self.analyze_image_data_url(question=question, data_url=data_url)

    async def analyze_image_data_url(self, *, question: str, data_url: str) -> str:
        if not self.api_key:
            raise VisionConfigError('Thiếu GEMINI_API_KEY.')

        content_type, image_bytes = self._parse_data_url(data_url)
        return await asyncio.to_thread(
            self._generate_content_sync,
            question=question,
            content_type=content_type,
            image_bytes=image_bytes,
        )
