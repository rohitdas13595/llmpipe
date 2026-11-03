# Third-party notices

llmpipe is AGPL-3.0. The pieces below are included under their own terms.
Close ports keep the upstream copyright in the file header and here.

## Pipecat

Portions of `textagg` (sentence boundaries, the aggregator lookahead, and the
markdown filter) are derived from Pipecat:

https://github.com/pipecat-ai/pipecat

`src/pipecat/utils/string.py`, `src/pipecat/utils/text/simple_text_aggregator.py`,
and `src/pipecat/utils/text/markdown_text_filter.py`.

```
BSD 2-Clause License

Copyright (c) 2024-2026, Daily

Redistribution and use in source and binary forms, with or without
modification, are permitted provided that the following conditions are met:

1. Redistributions of source code must retain the above copyright notice, this
   list of conditions and the following disclaimer.

2. Redistributions in binary form must reproduce the above copyright notice,
   this list of conditions and the following disclaimer in the documentation
   and/or other materials provided with the distribution.

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE
FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER
CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY,
OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
```

Portions of `audio/vad` (the Quiet/Starting/Speaking/Stopping machine), `audio/vad/silero` (the ONNX windowing and the quiet-state reset), `audio/turn/smartturn` (the 8 second ring and the ONNX call), and `turns` (start, stop, and mute strategies) are derived from Pipecat:

`src/pipecat/audio/vad/vad_analyzer.py`, `src/pipecat/audio/vad/silero.py`, `src/pipecat/audio/turn/smart_turn/base_smart_turn.py`, `src/pipecat/audio/turn/smart_turn/local_smart_turn_v3.py`, `src/pipecat/audio/turn/smart_turn/_whisper_features.py`, and `src/pipecat/turns/`.

The same BSD 2-Clause licence above covers those ports. The copyright line in each file names the upstream path.

`audio/turn/smartturn/features.go` also follows the numpy path of Hugging Face Transformers (`src/transformers/models/whisper/feature_extraction_whisper.py` and `src/transformers/audio_utils.py`), which Pipecat vendored. That math is Apache License 2.0, Copyright 2022 The HuggingFace Inc. team and Copyright 2023 The HuggingFace Inc. team and the librosa & torchaudio authors. The licence text is at https://www.apache.org/licenses/LICENSE-2.0.

## Silero VAD

The Silero weights (`silero_vad.onnx`, release v5.1.2) are not committed. `make models` downloads them. They are MIT, Copyright (c) 2020-present Silero Team. https://github.com/snakers4/silero-vad

```
MIT License

Copyright (c) 2020-present Silero Team

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
```

## Smart Turn

The Smart Turn v3.2 weights (`smart-turn-v3.2-cpu.onnx`) are not committed. `make models` downloads them from https://huggingface.co/pipecat-ai/smart-turn-v3. The `pipecat-ai/smart-turn` repository licence, verified against https://github.com/pipecat-ai/smart-turn/blob/main/LICENSE, is BSD 2-Clause, Copyright (c) 2024–2025, Daily. The weights are used under that licence and are not redistributed in this tree. The text matches the Pipecat BSD 2-Clause above, with the 2024–2025 copyright line.

## ONNX Runtime

Builds with `-tags onnx` link `github.com/yalue/onnxruntime_go` against ONNX Runtime 1.22.0. Both are MIT. https://github.com/microsoft/onnxruntime and https://github.com/yalue/onnxruntime_go. The shared library is supplied at runtime through `ONNXRUNTIME_LIB` and is not vendored here.

## Gonum

`audio/turn/smartturn` uses `gonum.org/v1/gonum/dsp/fourier` for the real FFT. Gonum is BSD 3-Clause. https://github.com/gonum/gonum
