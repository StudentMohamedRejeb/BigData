import pandas as pd
from kafka import KafkaConsumer
from collections import Counter
import string
import nltk
from nltk.corpus import stopwords
from nltk.stem import WordNetLemmatizer
import matplotlib.pyplot as plt

# Download necessary NLTK data
nltk.download('stopwords')
nltk.download('wordnet')

# Kafka Consumer initialization
consumer = KafkaConsumer('text-big', bootstrap_servers='localhost:9092', auto_offset_reset='earliest')

# Initialize necessary objects
word_count = Counter()
character_count = 0  # To track total characters
line_count = 0       # To track line number (cursor position)
word_frequency = {}  # To track frequency of top words

# Initialize NLTK objects
stop_words = set(stopwords.words('english'))  # Set of stop words
lemmatizer = WordNetLemmatizer()  # Word lemmatizer

# Function to preprocess text
def preprocess_text(text):
    # Convert to lowercase
    text = text.lower()
    
    # Remove punctuation
    text = text.translate(str.maketrans('', '', string.punctuation))
    
    # Split into words
    words = text.split()
    
    # Remove stop words and lemmatize words
    words = [lemmatizer.lemmatize(word) for word in words if word not in stop_words]
    
    return words

# Function to update statistics
def update_statistics(words, text):
    global character_count, line_count
    character_count += len(text)  # Add the length of the line to character count
    line_count += 1  # Increment the line count (cursor position)

    # Update word count
    for word in words:
        word_count[word] += 1

    # Update word frequency for top words (optional)
    for word in words:
        if word in word_frequency:
            word_frequency[word] += 1
        else:
            word_frequency[word] = 1

# Consume data and process it
for message in consumer:
    text = message.value.decode('utf-8')
    
    # Preprocess the text
    words = preprocess_text(text)

    # Update statistics
    update_statistics(words, text)

    # For demonstration, we'll stop after processing a few messages
    if line_count >= 100:  # Example: stop after processing 100 lines
        break

# Convert word count to DataFrame
word_count_df = pd.DataFrame.from_dict(word_count, orient='index', columns=['Count'])
word_count_df.reset_index(inplace=True)
word_count_df.columns = ['Word', 'Count']

# Save word count to CSV
word_count_df.to_csv('WordsList.csv', index=False)

# Print real-time statistics
print(f"Total characters processed: {character_count}")
print(f"Total lines processed (cursor position): {line_count}")

# Display the top 10 frequent words
top_10_words = sorted(word_frequency.items(), key=lambda x: x[1], reverse=True)[:10]
print("Top 10 Frequent Words:")
for word, freq in top_10_words:
    print(f"{word}: {freq}")

# Optionally, you could save the top 10 frequent words to a separate file or visualize it.

consumer.close()

# Function to plot the top N frequent words
def plot_word_frequency(top_words):
    words, freqs = zip(*top_words)  # Unzip the top words and their frequencies
    plt.bar(words, freqs)
    plt.xticks(rotation=45, ha="right")
    plt.title("Top 10 Frequent Words")
    plt.show()

# Display real-time chart of the top 10 frequent words
plot_word_frequency(top_10_words)
