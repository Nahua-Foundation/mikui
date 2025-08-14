import { useState } from 'react';
import { Button } from '../ui/button';
import { Plus, Edit, Trash2, CheckCircle, Circle } from 'lucide-react';
import { toast } from 'sonner';
import { KafkaCluster } from './types';
import { mockClusters } from './mockData';
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from '../ui/table';

interface ClusterArchiveProps {
  onCreateNew: () => void;
  onEditCluster: (cluster: KafkaCluster) => void;
  onConnectToCluster: (cluster: KafkaCluster) => void;
}

export function ClusterArchive({ onCreateNew, onEditCluster, onConnectToCluster }: ClusterArchiveProps) {
  const [clusters, setClusters] = useState<KafkaCluster[]>(mockClusters);

  const handleDeleteCluster = (clusterId: string) => {
    setClusters(prev => prev.filter(cluster => cluster.id !== clusterId));
    toast.success('Cluster deleted successfully');
  };

  const getSecurityBadgeColor = (protocol: string) => {
    switch (protocol) {
      case 'PLAINTEXT':
        return 'bg-red-900/30 text-red-400 border-red-800';
      case 'SSL':
        return 'bg-blue-900/30 text-blue-400 border-blue-800';
      case 'SASL_PLAINTEXT':
        return 'bg-yellow-900/30 text-yellow-400 border-yellow-800';
      case 'SASL_SSL':
        return 'bg-green-900/30 text-green-400 border-green-800';
      default:
        return 'bg-gray-900/30 text-gray-400 border-gray-800';
    }
  };

  return (
    <div className="bg-[#0f172b] h-full flex flex-col">
      {/* Header */}
      <div className="border-b border-[#314158] p-6">
        <div className="flex items-center justify-between">
          <div>
            <h1 className="font-['Fira_Code:Retina',_sans-serif] text-xl text-[#90a1b9] mb-2">
              Cluster Archive
            </h1>
            <p className="text-sm text-[#62748E]">
              Manage your Kafka cluster connections
            </p>
          </div>
          <Button
            onClick={onCreateNew}
            className="bg-[#ffb86a] text-[#0f172b] hover:bg-[#e5a860] font-['Fira_Code:Retina',_sans-serif]"
          >
            <Plus className="size-4 mr-2" />
            Create New
          </Button>
        </div>
      </div>

      {/* Clusters Table */}
      <div className="flex-1 overflow-auto p-6">
        <div className="bg-[#0f172b] border border-[#314158] rounded-lg overflow-hidden">
          <Table>
            <TableHeader>
              <TableRow className="border-[#314158] hover:bg-[#0f172b]">
                <TableHead className="font-['Fira_Code:Retina',_sans-serif] text-[#90a1b9] bg-[#0f172b]">
                  Status
                </TableHead>
                <TableHead className="font-['Fira_Code:Retina',_sans-serif] text-[#90a1b9] bg-[#0f172b]">
                  Name
                </TableHead>
                <TableHead className="font-['Fira_Code:Retina',_sans-serif] text-[#90a1b9] bg-[#0f172b]">
                  Brokers
                </TableHead>
                <TableHead className="font-['Fira_Code:Retina',_sans-serif] text-[#90a1b9] bg-[#0f172b]">
                  Security
                </TableHead>
                <TableHead className="font-['Fira_Code:Retina',_sans-serif] text-[#90a1b9] bg-[#0f172b] text-right">
                  Actions
                </TableHead>
              </TableRow>
            </TableHeader>
            <TableBody>
              {clusters.map((cluster) => (
                <TableRow
                  key={cluster.id}
                  className="border-[#314158] hover:bg-[#314158]/20 cursor-pointer transition-colors"
                  onClick={() => onConnectToCluster(cluster)}
                >
                  <TableCell className="py-4">
                    <div className="flex items-center">
                      {cluster.isActive ? (
                        <CheckCircle className="size-4 text-green-500" />
                      ) : (
                        <Circle className="size-4 text-[#62748E]" />
                      )}
                    </div>
                  </TableCell>
                  <TableCell className="py-4">
                    <div className="font-['Fira_Code:Retina',_sans-serif] text-slate-50">
                      {cluster.name}
                    </div>
                  </TableCell>
                  <TableCell className="py-4">
                    <div className="font-['Fira_Code:Retina',_sans-serif] text-[#90a1b9] text-sm max-w-xs truncate">
                      {cluster.brokers}
                    </div>
                  </TableCell>
                  <TableCell className="py-4">
                    <span
                      className={`px-2 py-1 rounded text-xs font-['Fira_Code:Retina',_sans-serif] border ${getSecurityBadgeColor(
                        cluster.securityProtocol
                      )}`}
                    >
                      {cluster.securityProtocol}
                    </span>
                  </TableCell>
                  <TableCell className="py-4 text-right">
                    <div className="flex items-center justify-end gap-2">
                      <Button
                        onClick={(e: any) => {
                          e.stopPropagation();
                          onEditCluster(cluster);
                        }}
                        variant="ghost"
                        size="sm"
                        className="text-[#90a1b9] hover:text-[#ffb86a] hover:bg-[#314158] p-2"
                      >
                        <Edit className="size-4" />
                      </Button>
                      <Button
                        onClick={(e: any) => {
                          e.stopPropagation();
                          handleDeleteCluster(cluster.id);
                        }}
                        variant="ghost"
                        size="sm"
                        className="text-[#90a1b9] hover:text-red-400 hover:bg-[#314158] p-2"
                      >
                        <Trash2 className="size-4" />
                      </Button>
                    </div>
                  </TableCell>
                </TableRow>
              ))}
            </TableBody>
          </Table>
        </div>

        {clusters.length === 0 && (
          <div className="text-center py-12">
            <div className="text-[#62748E] font-['Fira_Code:Retina',_sans-serif] mb-4">
              No clusters configured yet
            </div>
            <Button
              onClick={onCreateNew}
              className="bg-[#ffb86a] text-[#0f172b] hover:bg-[#e5a860] font-['Fira_Code:Retina',_sans-serif]"
            >
              <Plus className="size-4 mr-2" />
              Create Your First Cluster
            </Button>
          </div>
        )}
      </div>
    </div>
  );
}