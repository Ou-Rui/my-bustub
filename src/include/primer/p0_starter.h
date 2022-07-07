//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// p0_starter.h
//
// Identification: src/include/primer/p0_starter.h
//
// Copyright (c) 2015-2020, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <memory>
#include <stdexcept>

namespace bustub {

/*
 * The base class defining a Matrix
 */
template <typename T>
class Matrix {
 protected:
  Matrix(int rows, int cols) : rows_(rows), cols_(cols) { linear_ = new T[rows * cols]; }

  // # of rows in the matrix
  int rows_;
  // # of Columns in the matrix
  int cols_;
  // Flattened array containing the elements of the matrix
  // the array in the destructor.
  T *linear_;

 public:
  // Return the # of rows in the matrix
  virtual int GetRows() = 0;

  // Return the # of columns in the matrix
  virtual int GetColumns() = 0;

  // Return the (i,j)th  matrix element
  virtual T GetElem(int i, int j) = 0;

  // Sets the (i,j)th  matrix element to val
  virtual void SetElem(int i, int j, T val) = 0;

  // Sets the matrix elements based on the array arr
  virtual void MatImport(T *arr) = 0;

  virtual ~Matrix() { delete[] linear_; };
};

template <typename T>
class RowMatrix : public Matrix<T> {
 public:
  RowMatrix(int rows, int cols) : Matrix<T>(rows, cols) {
    data_ = new T *[rows];
    for (int i = 0; i < rows; ++i) {
      data_[i] = Matrix<T>::linear_ + cols * i;
    }
  }

  int GetRows() override { return Matrix<T>::rows_; }

  int GetColumns() override { return Matrix<T>::cols_; }

  T GetElem(int i, int j) override {
    if (i < 0 || i >= Matrix<T>::rows_ || j < 0 || j >= Matrix<T>::cols_) {
      return 0;
    }
    return data_[i][j];
  }

  void SetElem(int i, int j, T val) override {
    if (i < 0 || i >= Matrix<T>::rows_ || j < 0 || j >= Matrix<T>::cols_) {
      return;
    }
    data_[i][j] = val;
  }

  void MatImport(T *arr) override {
    for (int i = 0; i < Matrix<T>::rows_ * Matrix<T>::cols_; ++i) {
      Matrix<T>::linear_[i] = arr[i];
    }
  }

  ~RowMatrix() override { delete[] data_; }

 private:
  // 2D array containing the elements of the matrix in row-major format
  // to point to corresponding elements of the 'linear' array.
  // Don't forget to free up the array in the destructor.
  T **data_;
};

template <typename T>
class RowMatrixOperations {
 public:
  // Compute (mat1 + mat2) and return the result.
  // Return nullptr if dimensions mismatch for input matrices.
  static std::unique_ptr<RowMatrix<T>> AddMatrices(std::unique_ptr<RowMatrix<T>> mat1,
                                                   std::unique_ptr<RowMatrix<T>> mat2) {
    if (mat1->GetRows() != mat2->GetRows() || mat1->GetColumns() != mat2->GetColumns()) {
      return std::unique_ptr<RowMatrix<T>>(nullptr);
    }

    int rows = mat1->GetRows();
    int cols = mat1->GetColumns();
    auto res = std::make_unique<RowMatrix<T>>(rows, cols);

    for (int i = 0; i < rows; ++i) {
      for (int j = 0; j < cols; ++j) {
        int temp = mat1->GetElem(i, j) + mat2->GetElem(i, j);
        res->SetElem(i, j, temp);
      }
    }

    return res;
  }

  // Compute matrix multiplication (mat1 * mat2) and return the result.
  // Return nullptr if dimensions mismatch for input matrices.
  static std::unique_ptr<RowMatrix<T>> MultiplyMatrices(std::unique_ptr<RowMatrix<T>> mat1,
                                                        std::unique_ptr<RowMatrix<T>> mat2) {
    int row_a = mat1->GetRows();
    int col_a = mat1->GetColumns();
    int row_b = mat2->GetRows();
    int col_b = mat2->GetColumns();
    if (col_a != row_b) {
      return std::unique_ptr<RowMatrix<T>>(nullptr);
    }

    int row_res = row_a;
    int col_res = col_b;
    auto res = std::make_unique<RowMatrix<T>>(row_res, col_res);
    T tmp;
//    auto res = std::unique_ptr<RowMatrix<T>>(new RowMatrix<T>(row_res, col_res));
    for (int i = 0; i < row_res; ++i) {
      for (int j = 0; j < col_res; ++j) {
        tmp = 0;
        for (int k = 0; k < col_a; ++k) {
          tmp += mat1->GetElem(i, k) * mat2->GetElem(k, j);
        }
        res->SetElem(i, j, tmp);
      }
    }
    return res;
  }

  // Simplified GEMM (general matrix multiply) operation
  // Compute (matA * matB + matC). Return nullptr if dimensions mismatch for input matrices
  static std::unique_ptr<RowMatrix<T>> GemmMatrices(std::unique_ptr<RowMatrix<T>> matA,
                                                    std::unique_ptr<RowMatrix<T>> matB,
                                                    std::unique_ptr<RowMatrix<T>> matC) {
    auto tmp = MultiplyMatrices(move(matA), move(matB));
    if (tmp != nullptr) {
      return AddMatrices(tmp.get(), move(matC));
    }
    return std::unique_ptr<RowMatrix<T>>(nullptr);
  }
};
}  // namespace bustub
