from kafka import KafkaProducer, KafkaConsumer
import time
from collections import Counter
import string
import nltk
from nltk.corpus import stopwords
from nltk.stem import WordNetLemmatizer
import matplotlib.pyplot as plt
import pandas as pd

# Download necessary NLTK data
nltk.download('stopwords')
nltk.download('wordnet')

# Kafka Producer: Simulates streaming data to Kafka topic
def produce_data(file_path):
    producer = KafkaProducer(bootstrap_servers='localhost:9092')
    try:
        with open(file_path, 'r') as file:
            for line in file:
                producer.send('text-big', value=line.encode('utf-8'))
                time.sleep(1)  # Simulates real-time streaming
                print(f"Sent: {line.strip()}")
    except Exception as e:
        print(f"Error: {e}")
    finally:
        producer.close()

# Kafka Consumer Setup
consumer = KafkaConsumer('text-big', bootstrap_servers='localhost:9092', auto_offset_reset='earliest')

# Initialize counters and text-processing tools
word_count = Counter()
character_count = 0
line_count = 0
word_frequency = {}

stop_words = set(stopwords.words('english'))
lemmatizer = WordNetLemmatizer()

# Preprocess text: removes punctuation, converts to lowercase, removes stopwords, and lemmatizes
def preprocess_text(text):
    text = text.lower().translate(str.maketrans('', '', string.punctuation))
    words = text.split()
    return [lemmatizer.lemmatize(word) for word in words if word not in stop_words]

# Update statistics: updates character count, line count, and word counts
def update_statistics(words, text):
    global character_count, line_count
    character_count += len(text)
    line_count += 1

    for word in words:
        word_count[word] += 1
        word_frequency[word] = word_frequency.get(word, 0) + 1

# Display statistics: prints character count, line count, and top 10 words
def print_statistics():
    print(f"Total characters processed: {character_count}")
    print(f"Total lines processed (cursor position): {line_count}")
    top_10_words = sorted(word_frequency.items(), key=lambda x: x[1], reverse=True)[:10]
    print("Top 10 Frequent Words:")
    for word, freq in top_10_words:
        print(f"{word}: {freq}")

# Plot top N frequent words
def plot_word_frequency(top_words):
    words, freqs = zip(*top_words)
    plt.bar(words, freqs)
    plt.xticks(rotation=45, ha="right")
    plt.title("Top 10 Frequent Words")
    plt.show()

# Stream processing: consume messages, process, and update statistics
for message in consumer:
    text = message.value.decode('utf-8')
    words = preprocess_text(text)
    update_statistics(words, text)

    if line_count >= 1000:  # Stop after processing 1000 lines
        break

# Save word count to CSV
word_count_df = pd.DataFrame.from_dict(word_count, orient='index', columns=['Count']).reset_index()
word_count_df.columns = ['Word', 'Count']
word_count_df.to_csv('WordsList.csv', index=False)

# Display statistics and plot
print_statistics()
top_10_words = sorted(word_frequency.items(), key=lambda x: x[1], reverse=True)[:10]
plot_word_frequency(top_10_words)

# Close the consumer
consumer.close()

# Example usage: uncomment to stream file data to Kafka
# produce_data('Simple.txt')
