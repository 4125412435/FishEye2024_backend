import datetime
import time

import matplotlib.pyplot as plt
import torch
import torch.nn as nn
from torch.nn.utils.rnn import pad_sequence
from torch.utils.data import Dataset, DataLoader
from sklearn.metrics.pairwise import cosine_similarity
from tqdm import tqdm

import service

import datetime


def preprocess_data(transponderpings_dict, vocab_mapping, ship_mapping, time_interval, max_len):
    """
    Preprocesses the transponderping data for each ship and splits long sequences into smaller sequences of max_len.

    Args:
        transponderpings_dict (dict): Dictionary with ship ids as keys and lists of transponderping data as values.
        vocab_mapping (dict): Mapping of source regions to unique tokens.
        time_interval (int): Time interval for discretizing timestamps.
        max_len (int): Maximum sequence length for splitting.

    Returns:
        sequences (list of list): Preprocessed sequences of region tokens for each ship.
        labels (list of list): Corresponding next-region labels.
        ship_ids (list): Ship ids for each sequence.
    """
    sequences = []
    labels = []
    ship_ids = []

    for vessel_id, pings in transponderpings_dict.items():
        # Sort by time
        pings = sorted(pings, key=lambda x: x['time'])
        start_time = pings[0]['time']
        current_time = start_time

        # Generate a sequence of regions based on dwell times and time intervals
        seq = []
        seq.append(vocab_mapping[pings[0]['source']])

        for ping in pings:
            source = ping['source']
            dwell = ping['dwell']
            ping_time = ping['time']
            dwell_duration = datetime.timedelta(seconds=dwell)
            time_slots = (ping_time + dwell_duration - current_time).seconds // time_interval
            if time_slots != 0:
                seq.extend([vocab_mapping[source]] * time_slots)
                current_time += datetime.timedelta(seconds=time_slots * time_interval)

        # Split the long sequence into smaller sequences if it's too long
        for i in range(0, len(seq) - 1, max_len):
            input_seq = seq[i:i + max_len - 1]  # All except the last token
            target_seq = seq[i + 1:i + max_len]  # All except the first token

            # Only append if the sequence is long enough to have both input and target
            if len(input_seq) > 0 and len(target_seq) > 0:
                sequences.append(input_seq)
                labels.append(target_seq)
                ship_ids.append(ship_mapping[vessel_id])

    return sequences, labels, ship_ids


def collate_fn(batch):
    sequences, labels, ship_ids = zip(*batch)

    # 使用 pad_sequence 来填充序列，padding_value 设置为-1
    sequences_padded = pad_sequence(sequences, batch_first=True, padding_value=0)
    labels_padded = pad_sequence(labels, batch_first=True, padding_value=0)

    # 获取每个序列的实际长度
    sequence_lengths = torch.tensor([len(seq) for seq in sequences])  # 获取每个序列的长度
    mask = torch.arange(sequences_padded.size(1)).unsqueeze(0) < sequence_lengths.unsqueeze(1)

    ship_ids = torch.tensor(ship_ids, dtype=torch.long, device='cuda')

    return sequences_padded, labels_padded, ship_ids, mask.cuda()


class ShipRouteDataset(Dataset):
    def __init__(self, sequences, labels, ship_ids, vocab_size):
        self.sequences = sequences
        self.labels = labels
        self.ship_ids = ship_ids  # Now also receive ship_ids
        self.vocab_size = vocab_size

    def __len__(self):
        return len(self.sequences)

    def __getitem__(self, idx):
        return torch.tensor(self.sequences[idx], dtype=torch.long, device='cuda'), \
               torch.tensor(self.labels[idx], dtype=torch.long, device='cuda'), \
               torch.tensor(self.ship_ids[idx], dtype=torch.long, device='cuda')  # Return the ship ID


# 3. Transformer Model
class ShipRoutePredictor(nn.Module):
    def __init__(self, vocab_size, embed_dim, num_heads, num_layers, num_ships, dropout=0.1, max_seq_len=1024):
        super(ShipRoutePredictor, self).__init__()
        self.embedding = nn.Embedding(vocab_size, embed_dim)
        self.positional_encoding = nn.Parameter(torch.randn(1, max_seq_len, embed_dim))
        self.ship_id_embedding = nn.Embedding(num_ships, embed_dim)
        self.transformer = nn.TransformerEncoder(
            nn.TransformerEncoderLayer(d_model=embed_dim, nhead=num_heads, dropout=dropout, dim_feedforward=512,
                                       batch_first=True),
            num_layers=num_layers,
        )
        self.fc = nn.Linear(embed_dim, vocab_size)

    def forward(self, x, ship_ids):
        # x: (batch_size, seq_len), ship_ids: (batch_size,)
        embed = self.embedding(x) + self.positional_encoding[:, :x.size(1), :]
        ship_embed = self.ship_id_embedding(ship_ids).unsqueeze(1)
        embed = embed + ship_embed
        out = self.transformer(embed)  # (seq_len, batch_size, embed_dim)
        out = self.fc(out)  # (batch_size, seq_len, vocab_size)
        return out


def train_model(model, dataloader, criterion, optimizer, scheduler, epochs=10):
    model.train()
    loss_list = []
    for epoch in range(epochs):
        epoch_loss = 0
        for sequences, targets, ship_ids, mask in tqdm(dataloader):
            optimizer.zero_grad()
            outputs = model(sequences, ship_ids)  # (batch_size, seq_len, vocab_size)

            output_flat = outputs.view(-1, outputs.size(-1))
            target_flat = targets.view(-1)

            target_flat = target_flat.masked_fill(mask.view(-1) == 0, -1)
            mask_flat = mask.view(-1)
            loss = criterion(output_flat[mask_flat == 1], target_flat[mask_flat == 1])

            loss.backward()
            optimizer.step()
            epoch_loss += loss.item()

        # Update the learning rate with the scheduler
        scheduler.step()

        loss_list.append(epoch_loss / len(dataloader))
        print(f"Epoch {epoch + 1}/{epochs}, Loss: {epoch_loss / len(dataloader):.4f}", flush=True)

    plt.plot(range(1, epochs + 1), loss_list)
    plt.title('Train Loss')
    plt.xlabel('Epochs')
    plt.ylabel('Loss')
    plt.legend()
    plt.show()


# 5. Similarity Calculation
def calculate_similarity(model, ship_sequences, ship_ids):
    model.eval()
    embeddings = []
    with torch.no_grad():
        for seq, ship_id in zip(ship_sequences, ship_ids):
            seq = torch.tensor(seq, dtype=torch.long).unsqueeze(0)
            ship_id = torch.tensor([ship_id], dtype=torch.long)
            embedding = model.embedding(seq) + model.ship_id_embedding(ship_id).unsqueeze(1)
            embeddings.append(embedding.mean(dim=1).squeeze(0))
    embeddings = torch.stack(embeddings).numpy()
    return cosine_similarity(embeddings)


def calculate_similarity_static(model, ship_ids):
    model.eval()
    with torch.no_grad():
        embeddings = model.ship_id_embedding(torch.tensor(ship_ids, dtype=torch.long).cuda())
    return cosine_similarity(embeddings.cpu().numpy())


def get_ship_embedding(model, ship_ids):
    model.eval()
    with torch.no_grad():
        return model.ship_id_embedding(torch.tensor(ship_ids, dtype=torch.long))


if __name__ == "__main__":
    t1 = time.time()
    service.initialize('./data/MC2/mc2.json', geo_file_path='./data/MC2/Oceanus Information/Oceanus Geography.geojson')
    t2 = time.time()
    print('Cost ', t2 - t1, 's')

    vocab_mapping = {k: v + 1 for v, k in enumerate(service.id2location.keys())}
    ship_mapping = {k: v for v, k in enumerate(service.id2vessel.keys())}

    transponderpings_dict = service.select_transponder_ping()

    time_interval = 3600
    sequences, labels, ship_ids = preprocess_data(transponderpings_dict, vocab_mapping, ship_mapping, time_interval,
                                                  max_len=128)
    max_length = 0
    for s in sequences:
        if (len(s) > max_length):
            max_length = len(s)
    print('max length', max_length)
    vocab_size = len(vocab_mapping) + 1  # pad_token

    embed_dim = 64
    num_heads = 4
    num_layers = 2
    num_ships = len(transponderpings_dict.keys())
    dropout = 0.1

    dataset = ShipRouteDataset(sequences, labels, ship_ids, vocab_size)
    dataloader = DataLoader(dataset, batch_size=64, shuffle=True, collate_fn=collate_fn)

    # Model, optimizer, and loss function
    model = ShipRoutePredictor(vocab_size=vocab_size, embed_dim=embed_dim, num_heads=num_heads, num_layers=num_layers,
                               num_ships=num_ships, dropout=dropout, max_seq_len=max_length).cuda()
    print(vocab_size, embed_dim, num_heads, num_layers, num_ships, dropout, max_length)
    criterion = nn.CrossEntropyLoss()
    optimizer = torch.optim.Adam(model.parameters(), lr=2e-3)

    # Learning Rate Scheduler
    scheduler = torch.optim.lr_scheduler.StepLR(optimizer, step_size=5,
                                                gamma=0.8)  # Decay every 10 epochs by a factor of 0.5

    # Train the model
    train_model(model, dataloader, criterion, optimizer, scheduler, epochs=30)

    torch.save(model.state_dict(), 'model.pth')
