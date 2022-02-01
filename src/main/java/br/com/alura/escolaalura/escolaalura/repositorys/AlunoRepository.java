package br.com.alura.escolaalura.escolaalura.repositorys;

import java.util.ArrayList;
import java.util.List;

import org.bson.Document;
import org.bson.codecs.configuration.CodecProvider;
import org.bson.codecs.configuration.CodecRegistries;
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.codecs.pojo.PojoCodecProvider;
import org.bson.types.ObjectId;
import org.springframework.stereotype.Repository;

import com.mongodb.MongoClientSettings;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;

import br.com.alura.escolaalura.escolaalura.models.Aluno;

@Repository
public class AlunoRepository {
	
	private MongoClient cliente;
	private MongoDatabase bancoDeDados;
	
	public void salvar(Aluno aluno) {		
		criarConexao();
		MongoCollection<Aluno> alunos = bancoDeDados.getCollection("alunos", Aluno.class);
		if (aluno.getId() == null) {
			alunos.insertOne(aluno);
		} else {
			alunos.updateOne(Filters.eq("_id", aluno.getId()), new Document("$set", aluno));
		}
		
		fecharConexao();
	}
	
	public List<Aluno> obterTodosAlunos(){		
		criarConexao();
		MongoCollection<Aluno> alunos = bancoDeDados.getCollection("alunos", Aluno.class);
		
		MongoCursor<Aluno> resultados = alunos.find().iterator();
		
		List<Aluno> alunosEncontrados = popularAlunos(resultados);		
		fecharConexao();
		
		return alunosEncontrados;
	}
	
	public Aluno obterAlunoPor(String id) {
		criarConexao();
		MongoCollection<Aluno> alunos = this.bancoDeDados.getCollection("alunos", Aluno.class);
		Aluno aluno = alunos.find(Filters.eq("_id", new ObjectId(id))).first();
		fecharConexao();
		return aluno;
	}
	
	

	public List<Aluno> pesquisarPor(String nome) {
		criarConexao();
		MongoCollection<Aluno> alunoCollection = this.bancoDeDados.getCollection("alunos", Aluno.class);
		MongoCursor<Aluno> resultados = alunoCollection.find(Filters.eq("nome", nome), Aluno.class).iterator();
		List<Aluno> alunos = popularAlunos(resultados);
		
		fecharConexao();
		
		return alunos;
	}
	
	private CodecRegistry criarConexao() {
		CodecProvider codec = PojoCodecProvider.builder().automatic(true).build();
		CodecRegistry registro = CodecRegistries.fromRegistries(MongoClientSettings.getDefaultCodecRegistry(),
				CodecRegistries.fromProviders(codec));		
		
		String uri = "mongodb://localhost:27017";
		
		this.cliente = MongoClients.create(uri);
		this.bancoDeDados = cliente.getDatabase("test").withCodecRegistry(registro);
		return registro;
	}

	private void fecharConexao() {
		this.cliente.close();
	}
	
	private List<Aluno> popularAlunos(MongoCursor<Aluno> resultados) {
		List<Aluno> alunos = new ArrayList<>();
		while(resultados.hasNext()) {
			alunos.add(resultados.next());
		}		
		return alunos;
	}

	public List<Aluno> pesquisarPor(String classificacao, double nota) {
		criarConexao();
		
		MongoCollection<Aluno> alunoCollection = this.bancoDeDados.getCollection("alunos", Aluno.class);
		
		MongoCursor<Aluno> resultados = null;
		
		if (classificacao.equals("reprovados")) {
			resultados = alunoCollection.find(Filters.lt("notas", nota)).iterator();
		} else if (classificacao.equals("aprovados")) {
			resultados = alunoCollection.find(Filters.gte("notas", nota)).iterator();
		}
		
		List<Aluno> alunos = popularAlunos(resultados);
		
		fecharConexao();
		
		return alunos;
	}
	
	
	
}
