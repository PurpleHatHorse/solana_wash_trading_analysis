"""
Visualization Module
Creates charts and network graphs for wash trading analysis
"""

import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import networkx as nx
import sys
import os
from datetime import datetime

# Set style
sns.set_style("whitegrid")
plt.rcParams['figure.figsize'] = (12, 8)


class WashTradingVisualizer:
    """Create visualizations for wash trading analysis"""
    
    def __init__(self, csv_filename: str):
        filepath = f"data/processed/{csv_filename}"
        
        if not os.path.exists(filepath):
            raise FileNotFoundError(f"File not found: {filepath}")
        
        print(f"\nLoading data: {filepath}")
        self.df = pd.read_csv(filepath)
        self.df['timestamp'] = pd.to_datetime(self.df['timestamp'])
        
        self.output_dir = "outputs/visualizations"
        os.makedirs(self.output_dir, exist_ok=True)
        
        print(f"✓ Loaded {len(self.df):,} transfers")
    
    def create_all_visualizations(self):
        """Generate all visualizations"""
        
        print("\n" + "="*70)
        print("GENERATING VISUALIZATIONS")
        print("="*70 + "\n")
        
        self.plot_transfer_timeline()
        self.plot_volume_distribution()
        self.plot_hourly_activity()
        self.plot_top_addresses()
        self.plot_network_graph()
        
        print(f"\n✓ All visualizations saved to {self.output_dir}/")
    
    def plot_transfer_timeline(self):
        """Plot transfer activity over time"""
        
        print("[1/5] Creating transfer timeline...")
        
        # Daily aggregation
        daily = self.df.groupby(self.df['timestamp'].dt.date).agg({
            'transfer_id': 'count',
            'usd_value': 'sum'
        }).reset_index()
        daily.columns = ['date', 'transfer_count', 'total_volume']
        
        fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(14, 10))
        
        # Transfer count
        ax1.plot(daily['date'], daily['transfer_count'], marker='o', linewidth=2)
        ax1.set_title('Daily Transfer Count', fontsize=14, fontweight='bold')
        ax1.set_xlabel('Date')
        ax1.set_ylabel('Number of Transfers')
        ax1.grid(True, alpha=0.3)
        
        # Volume
        ax2.plot(daily['date'], daily['total_volume'], marker='o', 
                linewidth=2, color='green')
        ax2.set_title('Daily Transfer Volume (USD)', fontsize=14, fontweight='bold')
        ax2.set_xlabel('Date')
        ax2.set_ylabel('Total Volume (USD)')
        ax2.grid(True, alpha=0.3)
        
        plt.tight_layout()
        filename = f"{self.output_dir}/transfer_timeline.png"
        plt.savefig(filename, dpi=300, bbox_inches='tight')
        plt.close()
        
        print(f"   ✓ Saved: {filename}")
    
    def plot_volume_distribution(self):
        """Plot distribution of transfer sizes"""
        
        print("[2/5] Creating volume distribution...")
        
        fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(14, 6))
        
        # Histogram
        self.df['usd_value'].hist(bins=50, ax=ax1, edgecolor='black')
        ax1.set_title('Transfer Size Distribution', fontsize=14, fontweight='bold')
        ax1.set_xlabel('Transfer Size (USD)')
        ax1.set_ylabel('Frequency')
        ax1.set_yscale('log')
        
        # Box plot
        ax2.boxplot(self.df['usd_value'].dropna(), vert=True)
        ax2.set_title('Transfer Size Box Plot', fontsize=14, fontweight='bold')
        ax2.set_ylabel('Transfer Size (USD)')
        ax2.set_yscale('log')
        
        plt.tight_layout()
        filename = f"{self.output_dir}/volume_distribution.png"
        plt.savefig(filename, dpi=300, bbox_inches='tight')
        plt.close()
        
        print(f"   ✓ Saved: {filename}")
    
    def plot_hourly_activity(self):
        """Plot activity by hour of day"""
        
        print("[3/5] Creating hourly activity chart...")
        
        hourly = self.df.groupby('hour').size()
        
        fig, ax = plt.subplots(figsize=(12, 6))
        hourly.plot(kind='bar', ax=ax, color='steelblue', edgecolor='black')
        ax.set_title('Transfer Activity by Hour of Day', fontsize=14, fontweight='bold')
        ax.set_xlabel('Hour (UTC)')
        ax.set_ylabel('Number of Transfers')
        ax.grid(True, alpha=0.3, axis='y')
        
        plt.tight_layout()
        filename = f"{self.output_dir}/hourly_activity.png"
        plt.savefig(filename, dpi=300, bbox_inches='tight')
        plt.close()
        
        print(f"   ✓ Saved: {filename}")
    
    def plot_top_addresses(self):
        """Plot top addresses by volume"""
        
        print("[4/5] Creating top addresses chart...")
        
        # Combine from and to volumes
        from_volume = self.df.groupby('from_address')['usd_value'].sum()
        to_volume = self.df.groupby('to_address')['usd_value'].sum()
        total_volume = from_volume.add(to_volume, fill_value=0)
        top_20 = total_volume.nlargest(20)
        
        fig, ax = plt.subplots(figsize=(12, 8))
        
        # Truncate addresses for display
        labels = [f"{addr[:6]}...{addr[-4:]}" for addr in top_20.index]
        
        ax.barh(range(len(top_20)), top_20.values, color='coral', edgecolor='black')
        ax.set_yticks(range(len(top_20)))
        ax.set_yticklabels(labels)
        ax.set_xlabel('Total Volume (USD)')
        ax.set_title('Top 20 Addresses by Volume', fontsize=14, fontweight='bold')
        ax.invert_yaxis()
        ax.grid(True, alpha=0.3, axis='x')
        
        plt.tight_layout()
        filename = f"{self.output_dir}/top_addresses.png"
        plt.savefig(filename, dpi=300, bbox_inches='tight')
        plt.close()
        
        print(f"   ✓ Saved: {filename}")
    
    def plot_network_graph(self, max_nodes: int = 50):
        """Plot network graph of transfers"""
        
        print(f"[5/5] Creating network graph (top {max_nodes} nodes)...")
        
        # Build graph
        G = nx.DiGraph()
        
        # Get top addresses by volume
        from_volume = self.df.groupby('from_address')['usd_value'].sum()
        to_volume = self.df.groupby('to_address')['usd_value'].sum()
        total_volume = from_volume.add(to_volume, fill_value=0)
        top_addresses = set(total_volume.nlargest(max_nodes).index)
        
        # Add edges for top addresses only
        for _, row in self.df.iterrows():
            from_addr = row['from_address']
            to_addr = row['to_address']
            
            if pd.isna(from_addr) or pd.isna(to_addr):
                continue
            
            if from_addr in top_addresses or to_addr in top_addresses:
                if G.has_edge(from_addr, to_addr):
                    G[from_addr][to_addr]['weight'] += row['usd_value'] or 0
                else:
                    G.add_edge(from_addr, to_addr, weight=row['usd_value'] or 0)
        
        if G.number_of_nodes() == 0:
            print("   ⚠ No network to visualize")
            return
        
        fig, ax = plt.subplots(figsize=(16, 16))
        
        # Layout
        pos = nx.spring_layout(G, k=2, iterations=50, seed=42)
        
        # Node sizes based on degree
        node_sizes = [G.degree(node) * 100 for node in G.nodes()]
        
        # Edge widths based on weight
        edge_weights = [G[u][v]['weight'] for u, v in G.edges()]
        max_weight = max(edge_weights) if edge_weights else 1
        edge_widths = [1 + 5 * (w / max_weight) for w in edge_weights]
        
        # Draw
        nx.draw_networkx_nodes(G, pos, node_size=node_sizes, 
                              node_color='lightblue', alpha=0.7, ax=ax)
        nx.draw_networkx_edges(G, pos, width=edge_widths, 
                              alpha=0.3, edge_color='gray', 
                              arrows=True, arrowsize=10, ax=ax)
        
        # Add labels for high-degree nodes
        high_degree_nodes = [node for node in G.nodes() if G.degree(node) > 5]
        labels = {node: f"{node[:4]}..." for node in high_degree_nodes}
        nx.draw_networkx_labels(G, pos, labels, font_size=8, ax=ax)
        
        ax.set_title(f'Transfer Network Graph (Top {max_nodes} Addresses)', 
                    fontsize=16, fontweight='bold')
        ax.axis('off')
        
        plt.tight_layout()
        filename = f"{self.output_dir}/network_graph.png"
        plt.savefig(filename, dpi=300, bbox_inches='tight')
        plt.close()
        
        print(f"   ✓ Saved: {filename}")
        print(f"   Network: {G.number_of_nodes()} nodes, {G.number_of_edges()} edges")


def main():
    """Main execution"""
    
    if len(sys.argv) < 2:
        print("Usage: python visualize_results.py <processed_csv_filename>")
        sys.exit(1)
    
    csv_filename = sys.argv[1]
    
    try:
        visualizer = WashTradingVisualizer(csv_filename)
        visualizer.create_all_visualizations()
        
        print("\n✓ Visualization complete!")
        
    except Exception as e:
        print(f"\n✗ Error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()
