import numpy as np
import soundfile as sf

# 读取输入的音频文件
input_file = 'dc_offset_audio.wav'
audio_signal, sample_rate = sf.read(input_file)

# 定义一个函数，用于去除音频信号的直流偏移
def remove_mean(signal, frame_size):
    # 计算信号的总长度
    signal_length = len(signal)
    # 计算帧数，这里假设每帧包含frame_size个样本
    num_frames = signal_length // frame_size
    # 初始化一个与原信号相同大小的数组，用于存储去除均值后的信号
    mean_removed_signal = np.zeros_like(signal)
    
    # 对每帧信号进行处理
    for i in range(num_frames):
        # 计算当前帧的起始和结束索引
        start_index = i * frame_size
        end_index = start_index + frame_size
        # 计算当前帧的均值
        mean_value = np.mean(signal[start_index:end_index])
        # 去除当前帧的均值，并存储到结果数组中
        mean_removed_signal[start_index:end_index] = signal[start_index:end_index] - mean_value
    
    # 处理最后一帧（如果信号长度不是帧大小的整数倍）
    if signal_length % frame_size != 0:
        # 计算最后一帧的起始索引
        start_index = num_frames * frame_size
        # 计算最后一帧的均值，并去除均值
        mean_removed_signal[start_index:] = signal[start_index:] - np.mean(signal[start_index:])
    
    return mean_removed_signal

frame_size = 10
mean_removed_signal = remove_mean(audio_signal, frame_size)
output_file_mean = 'dc_mean_removed.wav'
sf.write(output_file_mean, mean_removed_signal, sample_rate)
print(f"均值去除后的音频已保存为 {output_file_mean}")