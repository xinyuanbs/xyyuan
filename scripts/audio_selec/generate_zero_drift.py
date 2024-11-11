import numpy as np
import soundfile as sf

# 参数
duration = 5  # 音频时长（秒）
sample_rate = 16000  # 采样率
frequency = 440  # 正弦波频率 (Hz)
dc_offset = 0.2  # 添加的直流偏移

# 生成带有零飘的正弦波信号
t = np.linspace(0, duration, int(sample_rate * duration), endpoint=False)
audio_signal = 0.5 * np.sin(2 * np.pi * frequency * t) + dc_offset

# 保存为 WAV 文件
sf.write("dc_offset_audio.wav", audio_signal, sample_rate)
print("带有零飘的音频已保存为 dc_offset_audio.wav")
