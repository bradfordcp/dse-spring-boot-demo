package com.datastax.example.demo.repository;

import static com.datastax.driver.core.querybuilder.QueryBuilder.bindMarker;
import static com.datastax.driver.core.querybuilder.QueryBuilder.eq;
import static org.springframework.data.cassandra.core.query.Criteria.where;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

import com.datastax.driver.core.*;
import com.datastax.driver.core.exceptions.DriverException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DataAccessException;
import org.springframework.data.cassandra.core.CassandraOperations;
import org.springframework.data.cassandra.core.cql.SessionCallback;
import org.springframework.data.cassandra.core.mapping.*;
import org.springframework.data.cassandra.core.query.Query;
import org.springframework.data.cassandra.repository.CassandraRepository;
import org.springframework.data.cassandra.repository.query.CassandraEntityInformation;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Slice;
import org.springframework.data.mapping.PersistentProperty;
import org.springframework.data.util.StreamUtils;
import org.springframework.data.util.Streamable;
import org.springframework.util.Assert;

import com.datastax.driver.core.querybuilder.Insert;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.querybuilder.Select;

/**
 * Repository implementation for Cassandra utilizing Prepared and Bound Statements
 *
 * @see CassandraRepository
 * @see org.springframework.data.cassandra.repository.support.SimpleCassandraRepository
 */
public class PreparedCassandraRepository<T, ID> implements CassandraRepository<T, ID> {

    Logger logger = LoggerFactory.getLogger(PreparedCassandraRepository.class);

    private final CassandraEntityInformation<T, ID> entityInformation;
    private final CassandraOperations operations;

    private final BasicCassandraPersistentEntity cassandraPersistentEntity;

    private final ConcurrentHashMap<String, PreparedStatement> preparedStatements;

    /**
     * Create a new {@link PreparedCassandraRepository} for the given {@link CassandraEntityInformation}
     * and {@link org.springframework.data.cassandra.core.CassandraTemplate}.
     *
     * @param metadata must not be {@literal null}.
     * @param operations must not be {@literal null}.
     */
    public PreparedCassandraRepository(CassandraEntityInformation<T, ID> metadata, CassandraOperations operations) {

        Assert.notNull(metadata, "CassandraEntityInformation must not be null");
        Assert.notNull(operations, "CassandraOperations must not be null");

        this.entityInformation = metadata;
        this.operations = operations;
        this.cassandraPersistentEntity = operations.getConverter().getMappingContext().getRequiredPersistentEntity(metadata.getJavaType());

        this.preparedStatements = new ConcurrentHashMap<>();

        // Prepare statements
        // TODO move elsewhere
        this.operations.getCqlOperations().execute(new SessionCallback<Object>() {
            @Override
            public Object doInSession(Session session) throws DriverException, DataAccessException {
                logger.info("Preparing queries");

                // Prepare the insert query
                List<String> insertColumns = new ArrayList<>();
                List<Object> insertMarkers = new ArrayList<>();

                cassandraPersistentEntity.doWithProperties((PersistentProperty persistentProperty) -> {
                    insertColumns.add(persistentProperty.getName());
                    insertMarkers.add(bindMarker(persistentProperty.getName()));
                });

                Insert save = QueryBuilder.insertInto(cassandraPersistentEntity.getTableName().toCql()).values(insertColumns, insertMarkers);
                preparedStatements.putIfAbsent("save", session.prepare(save));

                // Prepare the select by ID query
                List<String> selectColumns = new ArrayList<>();
                List<Object> selectMarkers = new ArrayList<>();

                cassandraPersistentEntity.doWithProperties((PersistentProperty persistentProperty) -> {
                    if (persistentProperty.isIdProperty()) {
                        selectColumns.add(persistentProperty.getName());
                        selectMarkers.add(bindMarker(persistentProperty.getName()));
                    }
                });

                Select.Where findById = QueryBuilder.select().all().from(cassandraPersistentEntity.getTableName().toCql()).where(eq(selectColumns.get(0), selectMarkers.get(0)));
                preparedStatements.putIfAbsent("findById", session.prepare(findById));

                return null;
            }
        });
    }

    /* (non-Javadoc)
     * @see org.springframework.data.repository.CrudRepository#save(S)
     */
    @Override
    public <S extends T> S save(S entity) {
        Assert.notNull(entity, "Entity must not be null");

        PreparedStatement preparedStatement = preparedStatements.get("save");
        BoundStatement insert = preparedStatement.bind();

        Map<String, Object> toInsert = new LinkedHashMap<>();
        operations.getConverter().write(entity, toInsert, cassandraPersistentEntity);
        Iterator properties = toInsert.entrySet().iterator();

        while(properties.hasNext()) {
            Map.Entry<String, Object> entry = (Map.Entry)properties.next();
            insert.set(entry.getKey(), entry.getValue(), preparedStatement.getCodecRegistry().codecFor(entry.getValue()));
        }

        operations.getCqlOperations().execute(insert);

        return entity;
    }

    /* (non-Javadoc)
     * @see org.springframework.data.repository.CrudRepository#saveAll(java.lang.Iterable)
     */
    @Override
    public <S extends T> List<S> saveAll(Iterable<S> entities) {

        Assert.notNull(entities, "The given Iterable of entities must not be null");

        List<S> result = new ArrayList<>();

        for (S entity : entities) {
            result.add(save(entity));
        }

        return result;
    }

    /* (non-Javadoc)
     * @see org.springframework.data.cassandra.repository.TypedIdCassandraRepository#insert(java.lang.Object)
     */
    @Override
    public <S extends T> S insert(S entity) {

        Assert.notNull(entity, "Entity must not be null");

        return save(entity);
    }

    /* (non-Javadoc)
     * @see org.springframework.data.cassandra.repository.TypedIdCassandraRepository#insert(java.lang.Iterable)
     */
    @Override
    public <S extends T> List<S> insert(Iterable<S> entities) {

        Assert.notNull(entities, "The given Iterable of entities must not be null");

        List<S> result = new ArrayList<>();

        for (S entity : entities) {
            result.add(save(entity));
        }

        return result;
    }

    /* (non-Javadoc)
     * @see org.springframework.data.repository.CrudRepository#findById(java.lang.Object)
     */
    @Override
    public Optional<T> findById(ID id) {

        Assert.notNull(id, "The given id must not be null");

        CassandraPersistentProperty idProperty = (CassandraPersistentProperty)cassandraPersistentEntity.getIdProperty();

        PreparedStatement preparedStatement = preparedStatements.get("findById");
        BoundStatement select = preparedStatement.bind()
                .set(idProperty.getColumnName().toCql(), id, preparedStatement.getCodecRegistry().codecFor(id));

        return Optional.ofNullable(operations.selectOne(select, entityInformation.getJavaType()));
    }

    /* (non-Javadoc)
     * @see org.springframework.data.repository.CrudRepository#existsById(java.lang.Object)
     */
    @Override
    public boolean existsById(ID id) {

        Assert.notNull(id, "The given id must not be null");

        return operations.exists(id, entityInformation.getJavaType());
    }

    /* (non-Javadoc)
     * @see org.springframework.data.repository.CrudRepository#count()
     */
    @Override
    public long count() {
        return operations.count(entityInformation.getJavaType());
    }

    /* (non-Javadoc)
     * @see org.springframework.data.repository.CrudRepository#findAll()
     */
    @Override
    public List<T> findAll() {

        Select select = QueryBuilder.select().all().from(entityInformation.getTableName().toCql());

        return operations.select(select, entityInformation.getJavaType());
    }

    /* (non-Javadoc)
     * @see org.springframework.data.repository.CrudRepository#findAll(java.lang.Iterable)
     */
    @Override
    public List<T> findAllById(Iterable<ID> ids) {

        Assert.notNull(ids, "The given Iterable of id's must not be null");

        List<ID> idCollection = Streamable.of(ids).stream().collect(StreamUtils.toUnmodifiableList());

        return operations.select(Query.query(where(entityInformation.getIdAttribute()).in(idCollection)),
                entityInformation.getJavaType());
    }

    /* (non-Javadoc)
     * @see org.springframework.data.cassandra.repository.CassandraRepository#findAll(org.springframework.data.domain.Pageable)
     */
    @Override
    public Slice<T> findAll(Pageable pageable) {

        Assert.notNull(pageable, "Pageable must not be null");

        return operations.slice(Query.empty().pageRequest(pageable), entityInformation.getJavaType());
    }

    /* (non-Javadoc)
     * @see org.springframework.data.repository.CrudRepository#deleteById(java.lang.Object)
     */
    @Override
    public void deleteById(ID id) {

        Assert.notNull(id, "The given id must not be null");

        operations.deleteById(id, entityInformation.getJavaType());
    }

    /* (non-Javadoc)
     * @see org.springframework.data.repository.CrudRepository#delete(java.lang.Object)
     */
    @Override
    public void delete(T entity) {

        Assert.notNull(entity, "The given entity must not be null");

        deleteById(entityInformation.getRequiredId(entity));
    }

    /* (non-Javadoc)
     * @see org.springframework.data.repository.CrudRepository#deleteAll(java.lang.Iterable)
     */
    @Override
    public void deleteAll(Iterable<? extends T> entities) {

        Assert.notNull(entities, "The given Iterable of entities must not be null");

        entities.forEach(operations::delete);
    }

    /* (non-Javadoc)
     * @see org.springframework.data.repository.CrudRepository#deleteAll()
     */
    @Override
    public void deleteAll() {
        operations.truncate(entityInformation.getJavaType());
    }
}