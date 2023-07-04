#include "GraphMatRuntime.h"

#include <chrono>

class ConnectedComponents : public GraphMat::GraphProgram<int, int, int> {
  public:
  ConnectedComponents() {
    this->activity = GraphMat::ALL_VERTICES;
    this->order = GraphMat::edge_direction::ALL_EDGES;
    this->process_message_requires_vertexprop = false;
  }

  void reduce_function(int &a, const int &b) const {
    a = std::min(a, b);
  }

  void process_message(const int &message, const int edge_val, const int &vertexprop, int &res) const {
    res = message;
  }

  void apply(const int &message_out, int &vertexprop) {
    vertexprop = std::min(vertexprop, message_out);
  }

  bool send_message(const int &vertexprop, int &message) const {
    message = vertexprop;
    return true;// TODO: what is this?
  }
};

void run_connected_components(char *filename, int num_expts) {
  GraphMat::Graph<int> G;
  G.ReadMTX(filename);

  for (int expt = 1; expt <= num_expts; expt++) {

    int num_vertices = G.getNumberOfVertices();
    ConnectedComponents cc;

    auto cc_tmp = GraphMat::graph_program_init(cc, G);

    for (int i = 1; i <= num_vertices; i++) {
      if (G.vertexNodeOwner(i)) {
        G.setVertexproperty(i, i);
      }
    }

    auto start = std::chrono::high_resolution_clock::now();

    GraphMat::run_graph_program(&cc, G, -1, &cc_tmp);

    auto end = std::chrono::high_resolution_clock::now();
    std::chrono::duration<double> elapsed = end - start;
    // print time in ms
    std::cout << "CC Time = " << elapsed.count() * 1000 << " ms" << std::endl;

    GraphMat::graph_program_clear(cc_tmp);

    // print number of connected components on master MPI rank
    if (GraphMat::get_global_myrank() == 0) {
      int num_connected_components = 0;
      for (int i = 1; i <= num_vertices; i++) {
        if (G.getVertexproperty(i) == i) {
          num_connected_components++;
        }
      }
      std::cout << "Number of connected components: " << num_connected_components << std::endl;
    }
  }
}

int main(int argc, char *argv[]) {
  if (argc < 2) {
    std::cout << "Usage: " << argv[0] << " <filename>" << std::endl;
    return -1;
  }

  int num_iter = atoi(argv[2]);
  int num_expts = atoi(argv[3]);

  MPI_Init(&argc, &argv);
  run_connected_components(argv[1], num_expts);
  MPI_Finalize();

  return 0;
}